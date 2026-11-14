"""
AWS S3 Service Module
Handles all AWS S3 operations including indexing and searching
OPTIMIZED for handling millions of files with parallel processing
"""
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from botocore.config import Config
from datetime import datetime, timedelta, timezone
import json
import os
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class S3Service:
    """Service class for AWS S3 operations."""
    
    def __init__(self, bucket_name: str, region_name: str = 'us-east-1', 
                 aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None,
                 endpoint_url: Optional[str] = None,
                 max_workers: int = 20,
                 max_pool_connections: int = 50):
        """
        Initialize S3 service with optimized connection pooling.
        
        Args:
            bucket_name: Name of the S3 bucket
            region_name: AWS region name
            aws_access_key_id: AWS access key (optional, will use environment if not provided)
            aws_secret_access_key: AWS secret key (optional, will use environment if not provided)
            endpoint_url: Custom S3 endpoint URL (for S3-compatible services like S3 Ninja)
            max_workers: Maximum concurrent workers for parallel processing (default: 20)
            max_pool_connections: Maximum HTTP connection pool size (default: 50)
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.max_workers = max_workers
        self.lock = threading.Lock()
        
        # Optimized boto3 configuration for large-scale operations
        boto_config = Config(
            region_name=region_name,
            max_pool_connections=max_pool_connections,
            retries={'max_attempts': 3, 'mode': 'adaptive'}
        )
        
        # Initialize S3 client with optimized config
        client_params = {'config': boto_config}
        
        if endpoint_url:
            client_params['endpoint_url'] = endpoint_url
        
        if aws_access_key_id and aws_secret_access_key:
            client_params['aws_access_key_id'] = aws_access_key_id
            client_params['aws_secret_access_key'] = aws_secret_access_key
        
        self.s3_client = boto3.client('s3', **client_params)
    
    def test_connection(self) -> Dict[str, any]:
        """
        Test S3 connection and bucket access.
        
        Returns:
            Dict with success status and message
        """
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            return {
                'success': True,
                'message': f'Successfully connected to bucket: {self.bucket_name}'
            }
        except NoCredentialsError:
            return {
                'success': False,
                'message': 'AWS credentials not found. Please configure credentials.'
            }
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                return {
                    'success': False,
                    'message': f'Bucket {self.bucket_name} does not exist'
                }
            elif error_code == '403':
                return {
                    'success': False,
                    'message': f'Access denied to bucket {self.bucket_name}'
                }
            else:
                return {
                    'success': False,
                    'message': f'Error: {str(e)}'
                }
    
    def _extract_timestamp_from_filename(self, key: str, fallback_timestamp: datetime) -> str:
        """
        Extract timestamp from filename if it follows log_YYYY-MM-DD_HH-MM-SS pattern.
        
        Args:
            key: S3 object key (filename)
            fallback_timestamp: LastModified timestamp to use if parsing fails
            
        Returns:
            ISO format timestamp string
        """
        try:
            # Extract filename from path
            filename = key.split('/')[-1]
            
            # Check if it matches log_YYYY-MM-DD_HH-MM-SS pattern
            if filename.startswith('log_') and filename.count('_') >= 2:
                parts = filename.split('_')
                if len(parts) >= 3:
                    date_part = parts[1]  # YYYY-MM-DD
                    time_part = parts[2].split('.')[0]  # HH-MM-SS (remove extension)
                    
                    # Parse the timestamp
                    timestamp_str = f"{date_part}_{time_part}"
                    parsed_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
                    
                    # Make it timezone-aware (UTC)
                    parsed_timestamp = parsed_timestamp.replace(tzinfo=timezone.utc)
                    
                    return parsed_timestamp.isoformat()
        except Exception as e:
            # If parsing fails, use fallback
            pass
        
        # Return fallback timestamp (LastModified from S3)
        return fallback_timestamp.isoformat()
    
    def _process_s3_page(self, page_data):
        """Process a single S3 page in parallel."""
        page_objects = []
        
        if 'Contents' not in page_data:
            return page_objects
        
        for obj in page_data['Contents']:
            # Skip directories
            if obj['Key'].endswith('/'):
                continue
            
            # Extract timestamp from filename if possible
            timestamp = self._extract_timestamp_from_filename(obj['Key'], obj['LastModified'])
            
            page_objects.append({
                'key': obj['Key'],
                'timestamp': timestamp,
                'size': obj['Size'],
                'etag': obj['ETag']
            })
        
        return page_objects
    
    def build_index_from_s3(self, prefix: str = '', index_file: str = 's3_index.json') -> List[Dict]:
        """
        Build index from S3 bucket with PARALLEL processing for speed.
        Processes pages in batches of 100 for optimal performance.
        
        Args:
            prefix: S3 prefix to filter objects (e.g., 'logs/')
            index_file: Path to save the index file
            
        Returns:
            List of indexed objects
        """
        print(f"🚀 Building full index from S3 bucket: {self.bucket_name}")
        print(f"⚙️  Using {self.max_workers} parallel workers")
        print(f"📦 Processing in batches of 100 pages")
        start_time = datetime.now(timezone.utc)
        
        try:
            # Fetch pages in batches
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            print("⚡ Fetching and processing S3 pages in batches...")
            
            index = []
            page_count = 0
            batch_size = 100
            page_batch = []
            
            for page in page_iterator:
                page_batch.append(page)
                page_count += 1
                
                # Process batch when it reaches 100 pages
                if len(page_batch) >= batch_size:
                    print(f"   📦 Processing batch: pages {page_count - batch_size + 1} - {page_count}...")
                    
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        futures = [executor.submit(self._process_s3_page, p) for p in page_batch]
                        
                        for future in as_completed(futures):
                            page_objects = future.result()
                            index.extend(page_objects)
                    
                    print(f"   ✅ Batch complete | Total: {len(index):,} objects indexed")
                    page_batch = []  # Clear batch
            
            # Process remaining pages (if any)
            if page_batch:
                print(f"   📦 Processing final batch: pages {page_count - len(page_batch) + 1} - {page_count}...")
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    futures = [executor.submit(self._process_s3_page, p) for p in page_batch]
                    
                    for future in as_completed(futures):
                        page_objects = future.result()
                        index.extend(page_objects)
                
                print(f"   ✅ Final batch complete | Total: {len(index):,} objects indexed")
            
            # Calculate scan duration
            scan_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Save index with metadata
            print("💾 Saving index to file...")
            self._save_index_with_metadata(index, index_file, scan_duration, scan_type='full')
            
            print(f"✅ Index built successfully!")
            print(f"   📊 Total pages: {page_count:,}")
            print(f"   📊 Total objects: {len(index):,}")
            print(f"   ⏱️  Duration: {scan_duration:.2f}s")
            print(f"   ⚡ Speed: {len(index)/scan_duration:.0f} objects/sec")
            return index
            
        except ClientError as e:
            print(f"❌ Error building index: {str(e)}")
            raise
    
    def _process_incremental_page(self, args):
        """Process a page for incremental refresh in parallel."""
        page, existing_objects, last_scan_time = args
        
        if 'Contents' not in page:
            return {'current': {}, 'new': 0, 'modified': 0}
        
        current_objects = {}
        new_count = 0
        modified_count = 0
        
        for obj in page['Contents']:
            if obj['Key'].endswith('/'):
                continue
            
            key = obj['Key']
            last_modified = obj['LastModified']
            
            # Ensure timezone-aware comparison
            if last_modified.tzinfo is None:
                last_modified = last_modified.replace(tzinfo=timezone.utc)
            
            timestamp = self._extract_timestamp_from_filename(key, last_modified)
            
            current_objects[key] = {
                'key': key,
                'timestamp': timestamp,
                'size': obj['Size'],
                'etag': obj['ETag']
            }
            
            # Check if object is new or modified
            if key not in existing_objects:
                new_count += 1
            elif last_modified > last_scan_time:
                if existing_objects.get(key, {}).get('etag') != obj['ETag']:
                    modified_count += 1
        
        return {'current': current_objects, 'new': new_count, 'modified': modified_count}
    
    def incremental_refresh(self, prefix: str = '', index_file: str = 's3_index.json', 
                           enable_deletion_check: bool = True) -> Dict[str, any]:
        """
        OPTIMIZED incremental refresh with parallel processing.
        Much faster for millions of files.
        
        Args:
            prefix: S3 prefix to filter objects
            index_file: Path to the index file
            enable_deletion_check: Whether to detect deleted objects
            
        Returns:
            Dict with refresh statistics and updated index
        """
        try:
            start_time = datetime.now(timezone.utc)
            
            # Load existing index with metadata
            existing_data = self._load_index_with_metadata(index_file)
            if not existing_data:
                print("❌ No existing index found. Performing full scan...")
                index = self.build_index_from_s3(prefix, index_file)
                return {
                    'success': True,
                    'scan_type': 'full',
                    'index': index,
                    'new_objects': len(index),
                    'modified_objects': 0,
                    'deleted_objects': 0,
                    'scan_duration': (datetime.now(timezone.utc) - start_time).total_seconds()
                }
            
            existing_index = existing_data['objects']
            last_scan_time_str = existing_data['metadata']['last_scan_time']
            last_scan_time = datetime.fromisoformat(last_scan_time_str)
            
            # Ensure last_scan_time is timezone-aware
            if last_scan_time.tzinfo is None:
                last_scan_time = last_scan_time.replace(tzinfo=timezone.utc)
            
            print(f"🔄 Performing OPTIMIZED incremental refresh")
            print(f"   Last scan: {last_scan_time}")
            print(f"   Using {self.max_workers} parallel workers")
            print(f"   📦 Processing in batches of 100 pages")
            
            # Create lookup for existing objects (fast)
            existing_objects = {obj['key']: obj for obj in existing_index}
            print(f"   Existing objects in index: {len(existing_objects):,}")
            
            # Fetch and process pages in batches
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            
            print("⚡ Fetching and processing S3 pages for comparison...")
            
            current_objects = {}
            total_new = 0
            total_modified = 0
            page_count = 0
            batch_size = 100
            page_batch = []
            
            for page in page_iterator:
                page_batch.append(page)
                page_count += 1
                
                # Process batch when it reaches 100 pages
                if len(page_batch) >= batch_size:
                    print(f"   📦 Processing batch: pages {page_count - batch_size + 1} - {page_count}...")
                    
                    with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        args_list = [(p, existing_objects, last_scan_time) for p in page_batch]
                        futures = [executor.submit(self._process_incremental_page, args) for args in args_list]
                        
                        for future in as_completed(futures):
                            result = future.result()
                            current_objects.update(result['current'])
                            total_new += result['new']
                            total_modified += result['modified']
                    
                    print(f"   ✅ Batch complete | Total: {len(current_objects):,} objects | New: {total_new} | Modified: {total_modified}")
                    page_batch = []  # Clear batch
            
            # Process remaining pages (if any)
            if page_batch:
                print(f"   📦 Processing final batch: pages {page_count - len(page_batch) + 1} - {page_count}...")
                
                with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                    args_list = [(p, existing_objects, last_scan_time) for p in page_batch]
                    futures = [executor.submit(self._process_incremental_page, args) for args in args_list]
                    
                    for future in as_completed(futures):
                        result = future.result()
                        current_objects.update(result['current'])
                        total_new += result['new']
                        total_modified += result['modified']
                
                print(f"   ✅ Final batch complete | Total: {len(current_objects):,} objects")
            
            # Detect deletions
            deleted_count = 0
            deleted_keys = []
            if enable_deletion_check:
                deleted_keys = set(existing_objects.keys()) - set(current_objects.keys())
                deleted_count = len(deleted_keys)
                if deleted_count > 0:
                    print(f"🗑️  Deleted objects: {deleted_count}")
            
            # Build updated index
            updated_index = list(current_objects.values())
            
            # Calculate scan duration
            scan_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Save updated index with metadata
            print("💾 Saving updated index...")
            self._save_index_with_metadata(updated_index, index_file, scan_duration, scan_type='incremental')
            
            print(f"✅ Incremental refresh completed!")
            print(f"   📊 Total pages: {page_count:,}")
            print(f"   ⏱️  Duration: {scan_duration:.2f}s")
            print(f"   📊 New: {total_new}, Modified: {total_modified}, Deleted: {deleted_count}")
            print(f"   ⚡ Speed: {len(updated_index)/scan_duration:.0f} objects/sec")
            
            return {
                'success': True,
                'scan_type': 'incremental',
                'index': updated_index,
                'new_objects': total_new,
                'modified_objects': total_modified,
                'deleted_objects': deleted_count,
                'deleted_keys': list(deleted_keys),
                'scan_duration': scan_duration
            }
            
        except ClientError as e:
            print(f"❌ Error during incremental refresh: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }
    
    def smart_refresh(self, prefix: str = '', index_file: str = 's3_index.json',
                     full_scan_interval: int = 86400, enable_deletion_check: bool = True) -> Dict[str, any]:
        """
        Smart refresh: decides between incremental or full scan based on time elapsed.
        
        Args:
            prefix: S3 prefix to filter objects
            index_file: Path to the index file
            full_scan_interval: Seconds after which to force full scan (default: 24 hours)
            enable_deletion_check: Whether to detect deleted objects
            
        Returns:
            Dict with refresh statistics and updated index
        """
        # Load existing index metadata
        existing_data = self._load_index_with_metadata(index_file)
        
        if not existing_data:
            # No existing index - do full scan
            print("No existing index. Performing full scan...")
            index = self.build_index_from_s3(prefix, index_file)
            return {
                'success': True,
                'scan_type': 'full',
                'reason': 'no_existing_index',
                'index': index
            }
        
        # Check if full scan is needed
        # Handle old index format that might not have 'last_full_scan'
        if 'last_full_scan' not in existing_data['metadata']:
            print("⚠️  Old index format detected - performing full scan to upgrade...")
            index = self.build_index_from_s3(prefix, index_file)
            return {
                'success': True,
                'scan_type': 'full',
                'reason': 'index_format_upgrade',
                'index': index
            }
        
        last_full_scan = datetime.fromisoformat(existing_data['metadata']['last_full_scan'])
        
        # Ensure last_full_scan is timezone-aware
        if last_full_scan.tzinfo is None:
            last_full_scan = last_full_scan.replace(tzinfo=timezone.utc)
        
        time_since_full_scan = (datetime.now(timezone.utc) - last_full_scan).total_seconds()
        
        if time_since_full_scan >= full_scan_interval:
            print(f"Full scan interval exceeded ({time_since_full_scan:.0f}s). Performing full scan...")
            index = self.build_index_from_s3(prefix, index_file)
            return {
                'success': True,
                'scan_type': 'full',
                'reason': 'interval_exceeded',
                'index': index
            }
        
        # Otherwise, do incremental refresh
        return self.incremental_refresh(prefix, index_file, enable_deletion_check)
    
    def _load_index_with_metadata(self, index_file: str) -> Optional[Dict]:
        """
        Load index file with metadata.
        
        Args:
            index_file: Path to the index file
            
        Returns:
            Dict with metadata and objects, or None if file doesn't exist
        """
        if not os.path.exists(index_file):
            return None
        
        try:
            with open(index_file, 'r') as f:
                data = json.load(f)
            
            # Handle old format (list) - convert to new format
            if isinstance(data, list):
                print("Converting old index format to new format with metadata...")
                current_time = datetime.now(timezone.utc).isoformat()
                return {
                    'metadata': {
                        'last_scan_time': current_time,
                        'last_full_scan': current_time,
                        'total_objects': len(data),
                        'scan_duration': 0,
                        'scan_type': 'full',
                        's3_bucket': self.bucket_name,
                        'index_version': '2.0'
                    },
                    'objects': data
                }
            
            # Handle new format but missing last_full_scan (upgrade from v1.x)
            if 'metadata' in data and 'last_full_scan' not in data['metadata']:
                print("⚠️  Upgrading index metadata to include last_full_scan...")
                last_scan = data['metadata'].get('last_scan_time', datetime.now(timezone.utc).isoformat())
                data['metadata']['last_full_scan'] = last_scan
            
            # New format with metadata
            return data
            
        except Exception as e:
            print(f"Error loading index: {str(e)}")
            return None
    
    def _save_index_with_metadata(self, index: List[Dict], index_file: str, 
                                 scan_duration: float, scan_type: str = 'full') -> None:
        """
        Save index with metadata.
        
        Args:
            index: List of indexed objects
            index_file: Path to save the index file
            scan_duration: Time taken for the scan (seconds)
            scan_type: Type of scan performed ('full' or 'incremental')
        """
        # Load existing metadata if available
        existing_data = self._load_index_with_metadata(index_file)
        
        current_time = datetime.now(timezone.utc).isoformat()
        
        if existing_data and scan_type == 'incremental':
            # Preserve last_full_scan from existing metadata
            last_full_scan = existing_data['metadata'].get('last_full_scan', current_time)
        else:
            # This is a full scan
            last_full_scan = current_time
        
        metadata = {
            'last_scan_time': current_time,
            'last_full_scan': last_full_scan,
            'total_objects': len(index),
            'scan_duration': round(scan_duration, 2),
            'scan_type': scan_type,
            's3_bucket': self.bucket_name,
            'index_version': '2.0'
        }
        
        data = {
            'metadata': metadata,
            'objects': index
        }
        
        with open(index_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def get_index_metadata(self, index_file: str) -> Optional[Dict]:
        """
        Get metadata from index file.
        
        Args:
            index_file: Path to the index file
            
        Returns:
            Metadata dict or None
        """
        data = self._load_index_with_metadata(index_file)
        return data['metadata'] if data else None
    
    def search_by_date_range(self, index: List[Dict], start_date: datetime, 
                            end_date: datetime) -> List[Dict]:
        """
        Search objects within a date range.
        
        Args:
            index: List of indexed objects
            start_date: Start date
            end_date: End date
            
        Returns:
            Filtered list of objects
        """
        return [
            obj for obj in index 
            if start_date <= datetime.fromisoformat(obj['timestamp']) <= end_date
        ]
    
    def search_older_than_n_days(self, index: List[Dict], n: int, 
                                 ref_date: Optional[datetime] = None) -> List[Dict]:
        """
        Search objects older than N days.
        
        Args:
            index: List of indexed objects
            n: Number of days
            ref_date: Reference date (defaults to now)
            
        Returns:
            Filtered list of objects
        """
        if not ref_date:
            ref_date = datetime.now(datetime.fromisoformat(index[0]['timestamp']).tzinfo)
        cutoff = ref_date - timedelta(days=n)
        return [
            obj for obj in index 
            if datetime.fromisoformat(obj['timestamp']) < cutoff
        ]
    
    def search_latest_n_days(self, index: List[Dict], n: int, 
                            ref_date: Optional[datetime] = None) -> List[Dict]:
        """
        Search objects from latest N days.
        
        Args:
            index: List of indexed objects
            n: Number of days
            ref_date: Reference date (defaults to now)
            
        Returns:
            Filtered list of objects
        """
        if not ref_date:
            ref_date = datetime.now(datetime.fromisoformat(index[0]['timestamp']).tzinfo)
        cutoff = ref_date - timedelta(days=n)
        return [
            obj for obj in index 
            if datetime.fromisoformat(obj['timestamp']) >= cutoff
        ]
    
    def multi_filter(self, results: List[Dict], prefix: str = '', 
                    suffix: str = '') -> List[Dict]:
        """
        Apply prefix and suffix filters.
        
        Args:
            results: List of objects to filter
            prefix: Key prefix filter
            suffix: Key suffix filter
            
        Returns:
            Filtered list of objects
        """
        if prefix:
            results = [obj for obj in results if obj['key'].startswith(prefix)]
        if suffix:
            results = [obj for obj in results if obj['key'].endswith(suffix)]
        return results
    
    def download_object(self, key: str, local_path: str) -> Dict[str, any]:
        """
        Download an object from S3.
        
        Args:
            key: S3 object key
            local_path: Local path to save the file
            
        Returns:
            Dict with success status and message
        """
        try:
            self.s3_client.download_file(self.bucket_name, key, local_path)
            return {
                'success': True,
                'message': f'Downloaded {key} to {local_path}'
            }
        except ClientError as e:
            return {
                'success': False,
                'message': f'Error downloading {key}: {str(e)}'
            }
    
    def delete_objects(self, keys: List[str]) -> Dict[str, any]:
        """
        Delete multiple objects from S3.
        
        Args:
            keys: List of S3 object keys to delete
            
        Returns:
            Dict with success status and message
        """
        try:
            objects = [{'Key': key} for key in keys]
            response = self.s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': objects}
            )
            
            deleted = len(response.get('Deleted', []))
            errors = len(response.get('Errors', []))
            
            return {
                'success': errors == 0,
                'message': f'Deleted {deleted} objects. {errors} errors.',
                'deleted': deleted,
                'errors': errors
            }
        except ClientError as e:
            return {
                'success': False,
                'message': f'Error deleting objects: {str(e)}'
            }
    
    def get_object_metadata(self, key: str) -> Dict[str, any]:
        """
        Get metadata for an S3 object.
        
        Args:
            key: S3 object key
            
        Returns:
            Dict with object metadata
        """
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return {
                'success': True,
                'metadata': {
                    'key': key,
                    'size': response['ContentLength'],
                    'last_modified': response['LastModified'].isoformat(),
                    'content_type': response.get('ContentType', 'unknown'),
                    'etag': response['ETag']
                }
            }
        except ClientError as e:
            return {
                'success': False,
                'message': f'Error getting metadata: {str(e)}'
            }
    
    def build_index_chunked(self, prefix: str = '', index_file: str = 's3_index.json',
                           chunk_size: int = 100_000, use_compression: bool = False) -> Dict[str, any]:
        """
        Build index from S3 in chunks (streaming) for large datasets (1M+ files).
        More memory efficient than build_index_from_s3 for massive buckets.
        
        Args:
            prefix: S3 prefix to filter objects
            index_file: Path to save the index file
            chunk_size: Number of objects to process in each chunk
            use_compression: Whether to use gzip compression for the index
            
        Returns:
            Dict with success status and statistics
        """
        try:
            import gzip
            
            print(f"Building chunked index from S3 bucket: {self.bucket_name}")
            start_time = datetime.now(timezone.utc)
            
            # Determine file extension based on compression
            if use_compression:
                index_file = index_file.replace('.json', '.json.gz') if not index_file.endswith('.gz') else index_file
            
            # Open file for writing (streaming mode)
            if use_compression:
                file_handle = gzip.open(index_file, 'wt', encoding='utf-8')
            else:
                file_handle = open(index_file, 'w', encoding='utf-8')
            
            # Write initial structure
            file_handle.write('{"metadata": ')
            
            # Prepare metadata (will update at end)
            metadata_placeholder = '{}' + ',\n'
            file_handle.write(metadata_placeholder)
            file_handle.write('"objects": [\n')
            
            # Process objects in chunks
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_count = 0
            total_objects = 0
            chunk_buffer = []
            first_object = True
            
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' not in page:
                    continue
                
                page_count += 1
                
                for obj in page['Contents']:
                    if obj['Key'].endswith('/'):
                        continue
                    
                    timestamp = self._extract_timestamp_from_filename(obj['Key'], obj['LastModified'])
                    
                    obj_data = {
                        'key': obj['Key'],
                        'timestamp': timestamp,
                        'size': obj['Size'],
                        'etag': obj['ETag']
                    }
                    
                    chunk_buffer.append(obj_data)
                    total_objects += 1
                    
                    # Write chunk when buffer is full
                    if len(chunk_buffer) >= chunk_size:
                        for item in chunk_buffer:
                            if not first_object:
                                file_handle.write(',\n')
                            file_handle.write(json.dumps(item, indent=2))
                            first_object = False
                        
                        chunk_buffer = []
                        print(f"Processed {total_objects:,} objects...")
                
                if page_count % 10 == 0:
                    print(f"Processing page {page_count}...")
            
            # Write remaining objects
            if chunk_buffer:
                for item in chunk_buffer:
                    if not first_object:
                        file_handle.write(',\n')
                    file_handle.write(json.dumps(item, indent=2))
                    first_object = False
            
            # Close objects array
            file_handle.write('\n]}\n')
            file_handle.close()
            
            # Calculate duration
            scan_duration = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            # Now update metadata at the beginning (need to rewrite file)
            # Read the entire file, update metadata, and rewrite
            if use_compression:
                with gzip.open(index_file, 'rt', encoding='utf-8') as f:
                    data = json.load(f)
            else:
                with open(index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
            
            # Update metadata
            data['metadata'] = {
                'last_scan_time': datetime.now(timezone.utc).isoformat(),
                'last_full_scan': datetime.now(timezone.utc).isoformat(),
                'total_objects': total_objects,
                'scan_duration': round(scan_duration, 2),
                'scan_type': 'full',
                's3_bucket': self.bucket_name,
                'index_version': '2.0',
                'compressed': use_compression
            }
            
            # Rewrite with metadata
            if use_compression:
                with gzip.open(index_file, 'wt', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
            else:
                with open(index_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, indent=2)
            
            file_size = os.path.getsize(index_file)
            file_size_mb = file_size / (1024 * 1024)
            
            print(f"\n{'='*60}")
            print(f"✅ Chunked index built successfully!")
            print(f"   Objects: {total_objects:,}")
            print(f"   Duration: {scan_duration:.2f}s")
            print(f"   Index size: {file_size_mb:.2f} MB")
            print(f"   Compressed: {use_compression}")
            print(f"   File: {index_file}")
            print(f"{'='*60}")
            
            return {
                'success': True,
                'total_objects': total_objects,
                'scan_duration': scan_duration,
                'index_file': index_file,
                'file_size_mb': file_size_mb,
                'compressed': use_compression
            }
            
        except Exception as e:
            print(f"Error building chunked index: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }


# Example usage and testing
if __name__ == '__main__':
    # Test with environment variables
    bucket_name = os.getenv('AWS_S3_BUCKET', 'your-bucket-name')
    
    # Initialize service
    s3_service = S3Service(bucket_name)
    
    # Test connection
    result = s3_service.test_connection()
    print(result)
    
    # Build index (uncomment to use)
    # index = s3_service.build_index_from_s3(prefix='logs/')
    # print(f"Indexed {len(index)} objects")
    
    # For large datasets (1M+), use chunked indexing:
    # result = s3_service.build_index_chunked(prefix='', chunk_size=100_000, use_compression=True)
    # print(result)
