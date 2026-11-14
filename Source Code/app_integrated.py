"""
Enhanced Flask Application with AWS Integration
Integrates config.py and s3_service.py for production use
OPTIMIZED for handling millions of files with parallel processing
NOW SUPPORTS: Async S3 operations and SQLite index
"""
import os
import json
import time
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, request, jsonify, send_file
from collections import Counter
import io
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import atexit

from config import get_config
from s3_service import S3Service
import boto3
from botocore.exceptions import ClientError

# Import IndexDB (required for optimized searches)
try:
    from index_db import IndexDB
    SQLITE_INDEX_AVAILABLE = True
except ImportError:
    SQLITE_INDEX_AVAILABLE = False

# NOTE: S3ServiceOptimized is imported lazily only when USE_OPTIMIZED_SERVICE=True
# This avoids import errors when aioboto3 is not installed
OPTIMIZED_SERVICE_AVAILABLE = False
S3ServiceOptimized = None

try:
    from s3_event_worker import S3EventWorker
    EVENT_WORKER_AVAILABLE = True
except ImportError:
    EVENT_WORKER_AVAILABLE = False
    print("⚠️  Event worker not available")

app = Flask(__name__)

# Load configuration
config = get_config()
app.config.from_object(config)

# Global index cache
index_cache = None
last_index_load = None
s3_service = None
s3_service_optimized = None
index_db = None
event_worker = None  # NEW: Event worker for real-time updates

# ---------- INDEXING ----------
def _process_directory_batch(args):
    """Process a batch of files in parallel for local indexing."""
    root, files, dataset_dir = args
    DATE_FORMAT = "%Y-%m-%d_%H-%M-%S"
    batch_results = []
    
    for file in files:
        if file.startswith("log_") and file.endswith(".txt"):
            try:
                ts_str = file.split("_")[1]
                time_str = file.split("_")[2]
                ts_full = f"{ts_str}_{time_str}"
                ts = datetime.strptime(ts_full, DATE_FORMAT)
            except Exception:
                continue
            
            # Use relative path from dataset_dir (preserves hierarchy)
            rel_path = os.path.relpath(os.path.join(root, file), dataset_dir)
            # Normalize path separators for consistency
            rel_path = rel_path.replace('\\', '/')
            
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
            
            batch_results.append({
                "key": rel_path,
                "timestamp": ts.isoformat(),
                "size": file_size,
                "etag": "mock-etag"
            })
    
    return batch_results

def build_index_local(dataset_dir, index_file, max_workers=20):
    """Build index from local mock data with PARALLEL processing. Optimized for millions of files."""
    start_time = datetime.now()
    
    print(f"🚀 Building local index from: {dataset_dir}")
    print(f"⚙️  Using {max_workers} parallel workers")
    
    # First, collect all directories and their files
    print("📂 Scanning directory structure...")
    all_batches = []
    total_files = 0
    
    for root, _, files in os.walk(dataset_dir):
        log_files = [f for f in files if f.startswith("log_") and f.endswith(".txt")]
        if log_files:
            all_batches.append((root, log_files, dataset_dir))
            total_files += len(log_files)
    
    print(f"✅ Found {len(all_batches)} directories with {total_files:,} log files")
    print(f"⚡ Processing files in parallel...")
    
    # Process batches in parallel
    index = []
    processed_batches = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_process_directory_batch, batch): i 
                  for i, batch in enumerate(all_batches)}
        
        for future in as_completed(futures):
            batch_results = future.result()
            index.extend(batch_results)
            processed_batches += 1
            
            # Progress update
            if processed_batches % 10 == 0 or processed_batches == len(all_batches):
                print(f"   Processed {processed_batches}/{len(all_batches)} directories | {len(index):,} files indexed")
    
    # Calculate scan duration
    scan_duration = (datetime.now() - start_time).total_seconds()
    
    print(f"✅ Index build complete!")
    print(f"   📊 Total files: {len(index):,}")
    print(f"   ⏱️  Duration: {scan_duration:.2f}s")
    print(f"   ⚡ Speed: {len(index)/scan_duration:.0f} files/sec")
    
    # Save with metadata
    current_time = datetime.now().isoformat()
    data = {
        "metadata": {
            "last_scan_time": current_time,
            "last_full_scan": current_time,
            "total_objects": len(index),
            "scan_duration": round(scan_duration, 2),
            "scan_type": "full",
            "data_source": dataset_dir,
            "index_version": "2.0"
        },
        "objects": index
    }
    
    print("💾 Saving index to file...")
    with open(index_file, "w") as f:
        json.dump(data, f, indent=2)
    
    return index

# ---------- SEARCH FUNCTIONS ----------
def load_index(index_file):
    """Load index from JSON file (handles both old and new format)."""
    with open(index_file, "r") as f:
        data = json.load(f)
    
    # Handle new format with metadata
    if isinstance(data, dict) and 'objects' in data:
        return data['objects']
    
    # Handle old format (just list)
    return data

def search_by_date_range(index, start_date, end_date):
    """Search objects within a date range."""
    results = []
    for obj in index:
        obj_timestamp = datetime.fromisoformat(obj["timestamp"])
        # Ensure comparison with timezone-aware datetime
        if obj_timestamp.tzinfo is None:
            obj_timestamp = obj_timestamp.replace(tzinfo=timezone.utc)
        if start_date <= obj_timestamp <= end_date:
            results.append(obj)
    return results

def search_older_than_n_days(index, n, ref_date=None):
    """Search objects older than N days."""
    if not ref_date:
        ref_date = datetime.now(timezone.utc)
    cutoff = ref_date - timedelta(days=n)
    
    results = []
    for obj in index:
        obj_timestamp = datetime.fromisoformat(obj["timestamp"])
        # Ensure comparison with timezone-aware datetime
        if obj_timestamp.tzinfo is None:
            obj_timestamp = obj_timestamp.replace(tzinfo=timezone.utc)
        if obj_timestamp < cutoff:
            results.append(obj)
    return results

def search_latest_n_days(index, n, ref_date=None):
    """Search objects from the latest N days."""
    if not ref_date:
        ref_date = datetime.now(timezone.utc)
    cutoff = ref_date - timedelta(days=n)
    
    results = []
    for obj in index:
        obj_timestamp = datetime.fromisoformat(obj["timestamp"])
        # Ensure comparison with timezone-aware datetime
        if obj_timestamp.tzinfo is None:
            obj_timestamp = obj_timestamp.replace(tzinfo=timezone.utc)
        if obj_timestamp >= cutoff:
            results.append(obj)
    return results

def multi_filter(results, prefix, suffix):
    """Apply prefix and suffix filters to search results."""
    if prefix:
        results = [obj for obj in results if obj['key'].startswith(prefix)]
    if suffix:
        results = [obj for obj in results if obj['key'].endswith(suffix)]
    return results

def get_index():
    """Get index from cache or load/build from file with smart refresh and optimized performance.
    
    Supports multiple index backends:
    - SQLite: Fast LOCAL queries (TRUE client-side, no network calls)
    - JSON: Traditional file-based indexing (in-memory)
    """
    global index_cache, last_index_load, s3_service, s3_service_optimized, index_db, event_worker
    
    # PRIORITY 1: Use SQLite if enabled (TRUE client-side search)
    if app.config.get('USE_SQLITE_INDEX', False):
        if not SQLITE_INDEX_AVAILABLE:
            print("❌ SQLite index not available. Install: pip install sqlite3")
            print("   Falling back to JSON index...")
        else:
            # Initialize SQLite index
            if index_db is None:
                db_file = app.config.get('SQLITE_DB_FILE', 's3_index.db')
                print(f"🗄️  Using SQLite index: {db_file} (TRUE CLIENT-SIDE SEARCH)")
                index_db = IndexDB(db_file)
                
                # Check if SQLite DB is empty and JSON index exists
                if index_db.get_total_count() == 0:
                    index_file = app.config.get('INDEX_FILE', 's3_index.json')
                    if os.path.exists(index_file):
                        print(f"📦 Importing index from {index_file} to SQLite...")
                        index_db.import_from_json(index_file)
                        print("✅ SQLite index ready!")
                
                # START EVENT WORKER if enabled (real-time S3 event processing)
                if app.config.get('ENABLE_EVENT_DRIVEN_UPDATES', False) and event_worker is None:
                    queue_url = app.config.get('SQS_QUEUE_URL')
                    
                    if queue_url and EVENT_WORKER_AVAILABLE:
                        print(f"\n🚀 Starting real-time event worker...")
                        event_worker = S3EventWorker(
                            queue_url=queue_url,
                            index_db=index_db,
                            poll_interval=app.config.get('SQS_POLL_INTERVAL', 5),
                            max_messages=app.config.get('SQS_MAX_MESSAGES', 10),
                            wait_time=app.config.get('SQS_WAIT_TIME', 20),
                            visibility_timeout=app.config.get('SQS_VISIBILITY_TIMEOUT', 30),
                            aws_region=app.config.get('AWS_REGION', 'us-east-1')
                        )
                        event_worker.start()
                        print("✅ Event worker started - index will auto-update on S3 uploads!\n")
                    elif not queue_url:
                        print("⚠️  Event-driven updates enabled but SQS_QUEUE_URL not configured")
                    elif not EVENT_WORKER_AVAILABLE:
                        print("⚠️  Event worker not available - check imports")
            
            # Return SQLite service
            return index_db
    
    # PRIORITY 2: Use JSON in-memory index (legacy)
    # Check cache validity
    if index_cache and last_index_load:
        elapsed = (datetime.now() - last_index_load).seconds
        cache_timeout = app.config.get('INDEX_CACHE_TIMEOUT', 300)
        if elapsed < cache_timeout:
            return index_cache
    
    index_file = app.config['INDEX_FILE']
    
    # Use mock data or AWS S3
    if app.config['USE_MOCK_DATA']:
        # Build or load local index
        if not os.path.exists(index_file):
            print("🔧 Building local index...")
            max_workers = app.config.get('LOCAL_INDEX_WORKERS', 20)
            index_cache = build_index_local(app.config['DATASET_DIR'], index_file, max_workers)
        else:
            index_cache = load_index(index_file)
    else:
        # Determine which S3 service to use
        use_optimized = app.config.get('USE_OPTIMIZED_SERVICE', False)
        
        if use_optimized:
            # Lazy import of S3ServiceOptimized only when needed
            global OPTIMIZED_SERVICE_AVAILABLE, S3ServiceOptimized
            if S3ServiceOptimized is None:
                try:
                    from s3_service_optimized import S3ServiceOptimized
                    OPTIMIZED_SERVICE_AVAILABLE = True
                except Exception as e:
                    OPTIMIZED_SERVICE_AVAILABLE = False
                    print(f"⚠️  Cannot use optimized service: {type(e).__name__}")
                    print(f"   Error: {str(e)}")
                    print(f"   Falling back to standard S3 service...")
            
            if OPTIMIZED_SERVICE_AVAILABLE:
                # Initialize optimized S3 service
                if s3_service_optimized is None:
                    max_workers = app.config.get('S3_MAX_WORKERS', 150)
                    max_pool_connections = app.config.get('S3_MAX_POOL_CONNECTIONS', 250)
                    
                    print(f"⚡ Initializing OPTIMIZED async S3 service (workers={max_workers})...")
                    s3_service_optimized = S3ServiceOptimized(
                        bucket_name=app.config['AWS_S3_BUCKET'],
                        region_name=app.config['AWS_REGION'],
                        aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'),
                        aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY'),
                        endpoint_url=app.config.get('S3_ENDPOINT_URL'),
                        max_workers=max_workers,
                        max_pool_connections=max_pool_connections
                    )
                
                # Use smart refresh if enabled
                if app.config.get('ENABLE_SMART_REFRESH', True) and os.path.exists(index_file):
                    print("🔄 Using optimized incremental refresh...")
                    result = s3_service_optimized.incremental_refresh_sync(
                        prefix=app.config.get('S3_PREFIX', ''),
                        index_file=index_file,
                        enable_deletion_check=app.config.get('ENABLE_DELETION_DETECTION', True)
                    )
                    if result['success']:
                        index_cache = result.get('index', [])
                        print(f"✅ Optimized refresh completed: {result.get('scan_type', 'unknown')} scan")
                    else:
                        print(f"❌ Optimized refresh failed: {result.get('error', 'unknown')}")
                        index_cache = load_index(index_file)
                elif not os.path.exists(index_file):
                    print("🔧 Building S3 index with OPTIMIZED service (first time)...")
                    index_cache = s3_service_optimized.build_index_sync(
                        prefix=app.config.get('S3_PREFIX', ''),
                        index_file=index_file
                    )
                else:
                    # Smart refresh disabled - just load from file
                    index_cache = load_index(index_file)
            else:
                # Optimized service not available, fall through to standard service
                use_optimized = False
        
        if not use_optimized or not OPTIMIZED_SERVICE_AVAILABLE:
            # Use standard S3 service (legacy)
            if s3_service is None:
                max_workers = app.config.get('S3_MAX_WORKERS', 20)
                max_pool_connections = app.config.get('S3_MAX_POOL_CONNECTIONS', 50)
                
                s3_service = S3Service(
                    bucket_name=app.config['AWS_S3_BUCKET'],
                    region_name=app.config['AWS_REGION'],
                    aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY'),
                    endpoint_url=app.config.get('S3_ENDPOINT_URL'),
                    max_workers=max_workers,
                    max_pool_connections=max_pool_connections
                )
            
            # Use smart refresh if enabled
            if app.config.get('ENABLE_SMART_REFRESH', True) and os.path.exists(index_file):
                print("🔄 Using smart refresh...")
                result = s3_service.smart_refresh(
                    prefix=app.config.get('S3_PREFIX', ''),
                    index_file=index_file,
                    full_scan_interval=app.config.get('FULL_SCAN_INTERVAL', 86400),
                    enable_deletion_check=app.config.get('ENABLE_DELETION_DETECTION', True)
                )
                if result['success']:
                    index_cache = result.get('index', [])
                    print(f"✅ Smart refresh completed: {result.get('scan_type', 'unknown')} scan")
                else:
                    print(f"❌ Smart refresh failed: {result.get('error', 'unknown')}")
                    index_cache = load_index(index_file)
            elif not os.path.exists(index_file):
                print("🔧 Building S3 index (first time)...")
                index_cache = s3_service.build_index_from_s3(
                    prefix=app.config.get('S3_PREFIX', ''),
                    index_file=index_file
                )
            else:
                # Smart refresh disabled - just load from file
                index_cache = load_index(index_file)
    
    last_index_load = datetime.now()
    return index_cache

# ---------- ROUTES ----------
@app.route('/')
def index():
    """Main page."""
    try:
        idx = get_index()
        
        # Check backend type and get total count
        if isinstance(idx, IndexDB):
            total_objects = idx.get_total_count()
        else:
            total_objects = len(idx)
        
        use_s3 = not app.config['USE_MOCK_DATA']
    except Exception as e:
        print(f"Error loading index: {e}")
        total_objects = 0
        use_s3 = False
    
    return render_template('index.html', total_objects=total_objects, use_s3=use_s3)

@app.route('/api/search/date-range', methods=['POST'])
def api_search_date_range():
    """API endpoint for date range search."""
    try:
        data = request.json
        start_str = data.get('start_date')
        end_str = data.get('end_date')
        prefix = data.get('prefix', '')
        suffix = data.get('suffix', '')
        
        # Parse dates and make them timezone-aware (UTC)
        start = datetime.strptime(start_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = (datetime.strptime(end_str, "%Y-%m-%d") + timedelta(days=1) - timedelta(seconds=1)).replace(tzinfo=timezone.utc)
        
        t0 = time.time()
        index = get_index()
        
        # Apply result limit to prevent browser crashes
        max_results = app.config.get('EXPORT_MAX_ROWS', 50000)
        
        # Check backend type and use appropriate search method
        if isinstance(index, IndexDB):
            # Use SQLite search with filters applied in SQL (OPTIMIZED)
            results = index.search_by_date_range(
                start.isoformat(), 
                end.isoformat(),
                limit=max_results,
                prefix=prefix,
                suffix=suffix
            )
        else:
            # Traditional list-based search (in-memory)
            results = search_by_date_range(index, start, end)
            results = multi_filter(results, prefix, suffix)
            results = results[:max_results]
        
        total_count = len(results)
        truncated = total_count >= max_results  # May be more results in DB
        
        t1 = time.time()
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'total_count': total_count,
            'truncated': truncated,
            'max_results': max_results,
            'search_time': round(t1 - t0, 3)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/search/older-than', methods=['POST'])
def api_search_older_than():
    """API endpoint for searching objects older than N days."""
    try:
        data = request.json
        
        # Validate request data
        if not data:
            return jsonify({
                'success': False,
                'error': 'No JSON data received'
            }), 400
        
        # Validate n_days parameter
        if 'n_days' not in data:
            return jsonify({
                'success': False,
                'error': 'Missing required parameter: n_days'
            }), 400
        
        n_days = int(data.get('n_days'))
        prefix = data.get('prefix', '')
        suffix = data.get('suffix', '')
        
        t0 = time.time()
        index = get_index()
        
        # Apply result limit to prevent browser crashes
        max_results = app.config.get('EXPORT_MAX_ROWS', 50000)
        
        # Check backend type and use appropriate search method
        if isinstance(index, IndexDB):
            # Use SQLite search with filters applied in SQL (OPTIMIZED)
            results = index.search_older_than_n_days(
                n_days,
                limit=max_results,
                prefix=prefix,
                suffix=suffix
            )
        else:
            # Traditional list-based search (in-memory)
            results = search_older_than_n_days(index, n_days)
            results = multi_filter(results, prefix, suffix)
            results = results[:max_results]
        
        total_count = len(results)
        truncated = total_count >= max_results  # May be more results in DB
        
        t1 = time.time()
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'total_count': total_count,
            'truncated': truncated,
            'max_results': max_results,
            'search_time': round(t1 - t0, 3)
        })
    except ValueError as e:
        return jsonify({
            'success': False,
            'error': f'Invalid parameter value: {str(e)}'
        }), 400
    except Exception as e:
        app.logger.error(f'Search error: {str(e)}', exc_info=True)
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@app.route('/api/search/latest', methods=['POST'])
def api_search_latest():
    """API endpoint for searching latest N days."""
    try:
        data = request.json
        n_days = int(data.get('n_days'))
        prefix = data.get('prefix', '')
        suffix = data.get('suffix', '')
        
        t0 = time.time()
        index = get_index()
        
        # Apply result limit to prevent browser crashes
        max_results = app.config.get('EXPORT_MAX_ROWS', 50000)
        
        # Check backend type and use appropriate search method
        if isinstance(index, IndexDB):
            # Use SQLite search with filters applied in SQL (OPTIMIZED)
            results = index.search_latest_n_days(
                n_days,
                limit=max_results,
                prefix=prefix,
                suffix=suffix
            )
        else:
            # Traditional list-based search (in-memory)
            results = search_latest_n_days(index, n_days)
            results = multi_filter(results, prefix, suffix)
            results = results[:max_results]
        
        total_count = len(results)
        truncated = total_count >= max_results  # May be more results in DB
        
        t1 = time.time()
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results),
            'total_count': total_count,
            'truncated': truncated,
            'max_results': max_results,
            'search_time': round(t1 - t0, 3)
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/stats/monthly')
def api_monthly_stats():
    """API endpoint for monthly statistics."""
    try:
        idx = get_index()
        
        # Check if index is SQLite
        if isinstance(idx, IndexDB):
            # For SQLite, query all items efficiently
            cursor = idx.conn.cursor()
            cursor.execute(
                'SELECT strftime(\'{0}\', timestamp) as month FROM objects'.format('%Y-%m')
            )
            months = [row[0] for row in cursor.fetchall()]
        else:
            # Traditional list-based stats
            months = [datetime.fromisoformat(obj['timestamp']).strftime('%Y-%m') for obj in idx]
        
        counter = Counter(months)
        months_sorted = sorted(counter.keys())
        
        return jsonify({
            'success': True,
            'labels': months_sorted,
            'data': [counter[m] for m in months_sorted]
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/refresh-index', methods=['POST'])
def api_refresh_index():
    """API endpoint to refresh the index with OPTIMIZED smart refresh support."""
    try:
        global index_cache, last_index_load, s3_service, s3_service_optimized
        
        # Get force_full parameter
        data = request.get_json() or {}
        force_full = data.get('force_full', False)
        
        start_time = time.time()
        
        if app.config['USE_MOCK_DATA']:
            max_workers = app.config.get('LOCAL_INDEX_WORKERS', 20)
            index_cache = build_index_local(
                app.config['DATASET_DIR'],
                app.config['INDEX_FILE'],
                max_workers
            )
            scan_type = 'full'
            stats = {}
        else:
            # Determine which S3 service to use
            use_optimized = app.config.get('USE_OPTIMIZED_SERVICE', False)
            
            if use_optimized:
                # Lazy import of S3ServiceOptimized only when needed
                global OPTIMIZED_SERVICE_AVAILABLE, S3ServiceOptimized
                if S3ServiceOptimized is None:
                    try:
                        from s3_service_optimized import S3ServiceOptimized
                        OPTIMIZED_SERVICE_AVAILABLE = True
                    except Exception as e:
                        OPTIMIZED_SERVICE_AVAILABLE = False
                        print(f"⚠️  Cannot use optimized service: {type(e).__name__}")
                        print(f"   Falling back to standard S3 service...")
                
                if OPTIMIZED_SERVICE_AVAILABLE:
                    # Use optimized async service
                    if s3_service_optimized is None:
                        max_workers = app.config.get('S3_MAX_WORKERS', 150)
                        max_pool_connections = app.config.get('S3_MAX_POOL_CONNECTIONS', 250)
                        
                        s3_service_optimized = S3ServiceOptimized(
                            bucket_name=app.config['AWS_S3_BUCKET'],
                            region_name=app.config['AWS_REGION'],
                            endpoint_url=app.config.get('S3_ENDPOINT_URL'),
                            max_workers=max_workers,
                            max_pool_connections=max_pool_connections
                        )
                    
                    # Use smart refresh or force full scan
                    if force_full or not app.config.get('ENABLE_SMART_REFRESH', True):
                        print("🔄 Performing OPTIMIZED full scan...")
                        index_cache = s3_service_optimized.build_index_sync(
                            prefix=app.config.get('S3_PREFIX', ''),
                            index_file=app.config['INDEX_FILE']
                        )
                        scan_type = 'full'
                        stats = {}
                    else:
                        print("⚡ Performing OPTIMIZED incremental refresh...")
                        result = s3_service_optimized.incremental_refresh_sync(
                            prefix=app.config.get('S3_PREFIX', ''),
                            index_file=app.config['INDEX_FILE'],
                            enable_deletion_check=app.config.get('ENABLE_DELETION_DETECTION', True)
                        )
                        
                        if result['success']:
                            index_cache = result.get('index', [])
                            scan_type = result.get('scan_type', 'unknown')
                            stats = {
                                'new_objects': result.get('new_objects', 0),
                                'modified_objects': result.get('modified_objects', 0),
                                'deleted_objects': result.get('deleted_objects', 0)
                            }
                        else:
                            return jsonify({
                                'success': False,
                                'error': result.get('error', 'Unknown error')
                            }), 400
                else:
                    # Optimized service not available, fall through to standard service
                    use_optimized = False
            
            if not use_optimized or not OPTIMIZED_SERVICE_AVAILABLE:
                # Use standard S3 service (legacy)
                if s3_service is None:
                    max_workers = app.config.get('S3_MAX_WORKERS', 20)
                    max_pool_connections = app.config.get('S3_MAX_POOL_CONNECTIONS', 50)
                    
                    s3_service = S3Service(
                        bucket_name=app.config['AWS_S3_BUCKET'],
                        region_name=app.config['AWS_REGION'],
                        endpoint_url=app.config.get('S3_ENDPOINT_URL'),
                        max_workers=max_workers,
                        max_pool_connections=max_pool_connections
                    )
                
                # Use smart refresh or force full scan
                if force_full or not app.config.get('ENABLE_SMART_REFRESH', True):
                    print("🔄 Performing full scan...")
                    index_cache = s3_service.build_index_from_s3(
                        prefix=app.config.get('S3_PREFIX', ''),
                        index_file=app.config['INDEX_FILE']
                    )
                    scan_type = 'full'
                    stats = {}
                else:
                    print("⚡ Performing smart refresh...")
                    result = s3_service.smart_refresh(
                        prefix=app.config.get('S3_PREFIX', ''),
                        index_file=app.config['INDEX_FILE'],
                        full_scan_interval=app.config.get('FULL_SCAN_INTERVAL', 86400),
                        enable_deletion_check=app.config.get('ENABLE_DELETION_DETECTION', True)
                    )
                    
                    if result['success']:
                        index_cache = result.get('index', [])
                        scan_type = result.get('scan_type', 'unknown')
                        stats = {
                            'new_objects': result.get('new_objects', 0),
                            'modified_objects': result.get('modified_objects', 0),
                            'deleted_objects': result.get('deleted_objects', 0)
                        }
                    else:
                        return jsonify({
                            'success': False,
                            'error': result.get('error', 'Unknown error')
                        }), 400
        
        last_index_load = datetime.now()
        elapsed = time.time() - start_time
        
        response_data = {
            'success': True,
            'message': f'Index refreshed successfully ({scan_type} scan)',
            'total_objects': len(index_cache),
            'scan_type': scan_type,
            'scan_duration': round(elapsed, 2)
        }
        
        # Add stats if available
        if stats:
            response_data.update(stats)
        
        return jsonify(response_data)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/export/csv', methods=['POST'])
def api_export_csv():
    """API endpoint to export results as CSV."""
    try:
        data = request.json
        results = data.get('results', [])
        
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Key', 'Timestamp'])
        for obj in results:
            writer.writerow([obj['key'], obj['timestamp']])
        
        output.seek(0)
        bytes_output = io.BytesIO(output.getvalue().encode('utf-8'))
        bytes_output.seek(0)
        
        return send_file(
            bytes_output,
            mimetype='text/csv',
            as_attachment=True,
            download_name=f's3_search_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
        )
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/visualization')
def visualization():
    """Visualization page."""
    return render_template('visualization.html')

@app.route('/api/s3/test-connection', methods=['GET'])
def api_test_s3_connection():
    """Test AWS S3 connection."""
    try:
        global s3_service
        
        if app.config['USE_MOCK_DATA']:
            return jsonify({
                'success': True,
                'mode': 'mock',
                'message': 'Using mock data - S3 connection not required',
                'data_source': app.config['DATASET_DIR']
            })
        
        if s3_service is None:
            s3_service = S3Service(
                bucket_name=app.config['AWS_S3_BUCKET'],
                region_name=app.config['AWS_REGION'],
                endpoint_url=app.config.get('S3_ENDPOINT_URL')
            )
        
        result = s3_service.test_connection()
        result['mode'] = 's3'
        result['bucket'] = app.config['AWS_S3_BUCKET']
        result['endpoint'] = app.config.get('S3_ENDPOINT_URL', 'AWS S3')
        return jsonify(result)
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/event-worker/stats', methods=['GET'])
def api_event_worker_stats():
    """Get event worker statistics."""
    global event_worker
    
    if event_worker is None:
        return jsonify({
            'success': False,
            'message': 'Event worker not running',
            'enabled': app.config.get('ENABLE_EVENT_DRIVEN_UPDATES', False)
        })
    
    stats = event_worker.get_stats()
    return jsonify({
        'success': True,
        'stats': stats
    })

# ---------- S3 OBJECT OPERATIONS (COPY/MOVE/DELETE) ----------
@app.route('/api/objects/delete', methods=['POST'])
def api_delete_objects():
    """Delete multiple objects from S3."""
    try:
        data = request.json
        keys = data.get('keys', [])
        
        if not keys:
            return jsonify({'success': False, 'error': 'No keys provided'}), 400
        
        if len(keys) > 1000:
            return jsonify({'success': False, 'error': 'Maximum 1000 objects per request'}), 400
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            region_name=app.config['AWS_REGION'],
            aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY')
        )
        
        bucket_name = app.config['AWS_S3_BUCKET']
        
        # Delete objects in batches (S3 allows max 1000 per request)
        deleted = []
        failed = []
        
        for i in range(0, len(keys), 1000):
            batch = keys[i:i+1000]
            delete_objects = [{'Key': key} for key in batch]
            
            try:
                response = s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': delete_objects}
                )
                
                # Track deleted and failed objects
                if 'Deleted' in response:
                    deleted.extend([obj['Key'] for obj in response['Deleted']])
                
                if 'Errors' in response:
                    for error in response['Errors']:
                        failed.append({
                            'key': error['Key'],
                            'error': error['Message']
                        })
            except ClientError as e:
                for key in batch:
                    failed.append({'key': key, 'error': str(e)})
        
        return jsonify({
            'success': True,
            'total': len(keys),
            'deleted': len(deleted),
            'failed': len(failed),
            'errors': failed
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/objects/copy', methods=['POST'])
def api_copy_objects():
    """Copy objects to another location."""
    try:
        data = request.json
        keys = data.get('keys', [])
        dest_bucket = data.get('destination_bucket')
        dest_prefix = data.get('destination_prefix', '')
        preserve_path = data.get('preserve_path', True)
        
        if not keys:
            return jsonify({'success': False, 'error': 'No keys provided'}), 400
        
        if len(keys) > 100:
            return jsonify({'success': False, 'error': 'Maximum 100 objects per request (use batch operations for more)'}), 400
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            region_name=app.config['AWS_REGION'],
            aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY')
        )
        
        source_bucket = app.config['AWS_S3_BUCKET']
        target_bucket = dest_bucket if dest_bucket else source_bucket
        
        # Validate destination bucket
        try:
            s3_client.head_bucket(Bucket=target_bucket)
        except ClientError:
            return jsonify({'success': False, 'error': f'Destination bucket "{target_bucket}" does not exist or is not accessible'}), 400
        
        # Copy objects
        succeeded = []
        failed = []
        
        for key in keys:
            try:
                # Determine destination key
                if preserve_path:
                    dest_key = f"{dest_prefix}{key}" if dest_prefix else key
                else:
                    filename = key.split('/')[-1]
                    dest_key = f"{dest_prefix}{filename}" if dest_prefix else filename
                
                # Copy object
                copy_source = {'Bucket': source_bucket, 'Key': key}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=target_bucket,
                    Key=dest_key
                )
                
                succeeded.append({
                    'key': key,
                    'status': 'copied',
                    'new_key': dest_key,
                    'new_bucket': target_bucket
                })
            except ClientError as e:
                failed.append({
                    'key': key,
                    'error': str(e)
                })
        
        return jsonify({
            'success': True,
            'total': len(keys),
            'succeeded': len(succeeded),
            'failed': len(failed),
            'results': succeeded,
            'errors': failed
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/objects/move', methods=['POST'])
def api_move_objects():
    """Move objects to another location (copy + delete)."""
    try:
        data = request.json
        keys = data.get('keys', [])
        dest_bucket = data.get('destination_bucket')
        dest_prefix = data.get('destination_prefix', '')
        preserve_path = data.get('preserve_path', True)
        
        if not keys:
            return jsonify({'success': False, 'error': 'No keys provided'}), 400
        
        if len(keys) > 100:
            return jsonify({'success': False, 'error': 'Maximum 100 objects per request'}), 400
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            region_name=app.config['AWS_REGION'],
            aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY')
        )
        
        source_bucket = app.config['AWS_S3_BUCKET']
        target_bucket = dest_bucket if dest_bucket else source_bucket
        
        # Validate destination bucket
        try:
            s3_client.head_bucket(Bucket=target_bucket)
        except ClientError:
            return jsonify({'success': False, 'error': f'Destination bucket "{target_bucket}" does not exist'}), 400
        
        # Move objects (copy then delete)
        copied = []
        deleted = []
        errors = []
        
        for key in keys:
            try:
                # Determine destination key
                if preserve_path:
                    dest_key = f"{dest_prefix}{key}" if dest_prefix else key
                else:
                    filename = key.split('/')[-1]
                    dest_key = f"{dest_prefix}{filename}" if dest_prefix else filename
                
                # Copy object
                copy_source = {'Bucket': source_bucket, 'Key': key}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=target_bucket,
                    Key=dest_key
                )
                copied.append(key)
                
                # Delete original (only if copy succeeded)
                s3_client.delete_object(Bucket=source_bucket, Key=key)
                deleted.append(key)
                
            except ClientError as e:
                errors.append({
                    'key': key,
                    'error': str(e),
                    'stage': 'copy' if key not in copied else 'delete'
                })
        
        return jsonify({
            'success': True,
            'total': len(keys),
            'copied': len(copied),
            'deleted': len(deleted),
            'errors': errors
        })
    
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400

@app.route('/api/buckets/validate', methods=['GET'])
def api_validate_bucket():
    """Validate if a bucket exists and is accessible."""
    try:
        bucket_name = request.args.get('bucket')
        
        if not bucket_name:
            return jsonify({'valid': False, 'error': 'No bucket name provided'}), 400
        
        s3_client = boto3.client(
            's3',
            region_name=app.config['AWS_REGION'],
            aws_access_key_id=app.config.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=app.config.get('AWS_SECRET_ACCESS_KEY')
        )
        
        try:
            response = s3_client.head_bucket(Bucket=bucket_name)
            
            # Try to get bucket location
            location_response = s3_client.get_bucket_location(Bucket=bucket_name)
            region = location_response.get('LocationConstraint', 'us-east-1')
            
            return jsonify({
                'valid': True,
                'accessible': True,
                'region': region if region else 'us-east-1'
            })
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return jsonify({'valid': False, 'error': 'Bucket does not exist'}), 404
            elif error_code == '403':
                return jsonify({'valid': False, 'error': 'Access denied'}), 403
            else:
                return jsonify({'valid': False, 'error': str(e)}), 400
    
    except Exception as e:
        return jsonify({'valid': False, 'error': str(e)}), 400

# ---------- ERROR HANDLERS ----------
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

@app.errorhandler(500)
def internal_error(error):
    return render_template('500.html'), 500

# ---------- CLEANUP ----------
def cleanup():
    """Cleanup resources on shutdown."""
    global event_worker, index_db
    
    if event_worker:
        print("\n🛑 Shutting down event worker...")
        event_worker.stop()
    
    if index_db:
        print("🛑 Closing SQLite connection...")
        index_db.close()

# Register cleanup on exit
atexit.register(cleanup)

# ---------- MAIN ----------
if __name__ == '__main__':
    # Ensure index exists
    try:
        idx = get_index()

        if isinstance(idx, IndexDB):
            total_indexed = idx.get_total_count()
            mode = 'SQLite (Client-Side Index)'
        else:
            total_indexed = len(idx)
            mode = 'Mock Data (Local Files)' if app.config['USE_MOCK_DATA'] else 'AWS S3 / S3 Ninja'

        print(f"\n{'='*60}")
        print(f"S3 Search Engine started with {total_indexed:,} objects indexed")
        print(f"Mode: {mode}")
        
        if not app.config['USE_MOCK_DATA']:
            print(f"S3 Bucket: {app.config['AWS_S3_BUCKET']}")
            print(f"S3 Region: {app.config['AWS_REGION']}")
            if app.config.get('S3_ENDPOINT_URL'):
                print(f"S3 Endpoint: {app.config['S3_ENDPOINT_URL']} (S3 Ninja)")
            else:
                print(f"S3 Endpoint: AWS S3 (Production)")
        else:
            print(f"Data Directory: {app.config['DATASET_DIR']}")
        
        print(f"{'='*60}\n")
    except Exception as e:
        print(f"Warning: Could not load index: {e}")
    
    # Run on localhost only for better compatibility
    print("\n🌐 Starting Flask server...")
    print("   Open your browser and go to: http://localhost:8080")
    app.run(debug=app.config['DEBUG'], host='localhost', port=8080)
