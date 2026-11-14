import os
import random
import string
from datetime import datetime, timedelta
import sys
import time

# ---------- CONFIG ----------
OUTPUT_DIR = "mock_s3_bucket"

# Configurable file count - Use 3M for production, smaller for testing

NUM_FILES = int(os.getenv('NUM_FILES', 3_000_000))  # 3 Million files default

# Date range - 7 years (2019 to 2026)
START_DATE = datetime(2019, 1, 1)
END_DATE = datetime(2026, 2, 15)

# Hierarchical structure: Organize by year/month for better performance
USE_HIERARCHICAL = True  # year/month/log_*.txt structure

# Batch settings for memory optimization
BATCH_SIZE = 10_000  # Create files in batches
PROGRESS_INTERVAL = 50_000  # Show progress every 50K files

# File content size (bytes) - small to save disk space
CONTENT_SIZE = 100  # ~100 bytes per file

# ---------- FUNCTIONS ----------
def random_timestamp(start, end):
    """Generate a random datetime between two datetimes."""
    delta = end - start
    rand_seconds = random.randint(0, int(delta.total_seconds()))
    return start + timedelta(seconds=rand_seconds)

def get_file_key(timestamp, use_hierarchical=True):
    """Create S3-style key with optional hierarchical structure."""
    random_suffix = ''.join(random.choices(string.ascii_lowercase + string.digits, k=6))
    filename = f"log_{timestamp.strftime('%Y-%m-%d_%H-%M-%S')}_{random_suffix}.txt"
    
    if use_hierarchical:
        # Hierarchical: year/month/log_*.txt
        year = timestamp.strftime('%Y')
        month = timestamp.strftime('%m')
        return os.path.join(year, month, filename)
    else:
        # Flat structure: log_*.txt
        return filename

def format_time(seconds):
    """Format seconds into human-readable time."""
    hours, remainder = divmod(int(seconds), 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        return f"{hours}h {minutes}m {seconds}s"
    elif minutes > 0:
        return f"{minutes}m {seconds}s"
    else:
        return f"{seconds}s"

def format_size(bytes_size):
    """Format bytes into human-readable size."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_size < 1024:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024
    return f"{bytes_size:.2f} TB"

def create_files_batch(start_idx, count, total_files):
    """Create a batch of files with optimized I/O."""
    created = 0
    total_bytes = 0
    
    # Pre-generate timestamps and keys for this batch (faster)
    batch_data = []
    for _ in range(count):
        ts = random_timestamp(START_DATE, END_DATE)
        key = get_file_key(ts, USE_HIERARCHICAL)
        batch_data.append((ts, key))
    
    # Create files
    for ts, key in batch_data:
        file_path = os.path.join(OUTPUT_DIR, key)
        
        # Create parent folders if needed
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Write minimal content to save disk space
        content = f"Mock log file | Timestamp: {ts.isoformat()} | ID: {key}\n"
        # Pad to CONTENT_SIZE if needed
        if len(content) < CONTENT_SIZE:
            content += "." * (CONTENT_SIZE - len(content))
        
        with open(file_path, "w") as f:
            f.write(content)
        
        total_bytes += len(content)
        
        # Set actual OS file timestamp
        mod_time = ts.timestamp()
        os.utime(file_path, (mod_time, mod_time))
        
        created += 1
    
    return created, total_bytes

# ---------- MAIN ----------
if __name__ == "__main__":
    print("\n" + "="*70)
    print("  MOCK S3 DATASET GENERATOR - OPTIMIZED FOR LARGE SCALE")
    print("="*70)
    print(f"📊 Configuration:")
    print(f"   Target files: {NUM_FILES:,}")
    print(f"   Date range: {START_DATE.date()} to {END_DATE.date()} ({(END_DATE-START_DATE).days/365:.1f} years)")
    print(f"   Structure: {'Hierarchical (year/month/)' if USE_HIERARCHICAL else 'Flat'}")
    print(f"   Batch size: {BATCH_SIZE:,}")
    print(f"   Output: {OUTPUT_DIR}/")
    print(f"   File size: ~{CONTENT_SIZE} bytes each")
    estimated_size = (NUM_FILES * CONTENT_SIZE)
    print(f"   Estimated total: {format_size(estimated_size)}")
    print("="*70)
    
    # Estimate time
    if NUM_FILES >= 1_000_000:
        est_minutes = (NUM_FILES / 1_000_000) * 30  # ~30 min per million
        print(f"\n⏱️  Estimated time: {format_time(est_minutes * 60)}")
        print("   (You can reduce NUM_FILES for faster testing)")
    
    # Confirm for large datasets
    if NUM_FILES >= 100_000:
        response = input(f"\n⚠️  Generate {NUM_FILES:,} files? This may take time. (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Cancelled.")
            sys.exit(0)
    
    print("\n🚀 Starting generation...\n")
    
    # Create output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    start_time = time.time()
    total_created = 0
    total_bytes = 0
    
    # Process in batches
    num_batches = (NUM_FILES + BATCH_SIZE - 1) // BATCH_SIZE
    
    for batch_idx in range(num_batches):
        batch_start = batch_idx * BATCH_SIZE
        batch_count = min(BATCH_SIZE, NUM_FILES - batch_start)
        
        batch_created, batch_bytes = create_files_batch(batch_start, batch_count, NUM_FILES)
        
        total_created += batch_created
        total_bytes += batch_bytes
        
        # Progress update
        if total_created % PROGRESS_INTERVAL == 0 or total_created == NUM_FILES:
            elapsed = time.time() - start_time
            rate = total_created / elapsed if elapsed > 0 else 0
            remaining = (NUM_FILES - total_created) / rate if rate > 0 else 0
            
            progress = (total_created / NUM_FILES) * 100
            print(f"[{progress:5.1f}%] {total_created:,} / {NUM_FILES:,} files | "
                  f"Rate: {rate:,.0f} files/s | "
                  f"ETA: {format_time(remaining)} | "
                  f"Size: {format_size(total_bytes)}")
    
    # Final statistics
    elapsed = time.time() - start_time
    print("\n" + "="*70)
    print("✅ GENERATION COMPLETE!")
    print("="*70)
    print(f"📁 Files created: {total_created:,}")
    print(f"💾 Total size: {format_size(total_bytes)}")
    print(f"⏱️  Time taken: {format_time(elapsed)}")
    print(f"⚡ Average rate: {total_created/elapsed:,.0f} files/second")
    print(f"📂 Location: {OUTPUT_DIR}/")
    if USE_HIERARCHICAL:
        print(f"📊 Structure: {(END_DATE.year - START_DATE.year + 1)} years × 12 months")
    print("="*70 + "\n")
