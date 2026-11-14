"""
Test Event-Driven Index Updates
Quick test script to verify S3 → SQS → Worker → SQLite pipeline
"""
import boto3
import time
import requests
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Configuration
BUCKET_NAME = os.getenv('AWS_S3_BUCKET', 'samsung-prism-logs')
REGION = os.getenv('AWS_REGION', 'us-east-1')
FLASK_URL = 'http://localhost:8080'

# Create test file
def create_test_file():
    """Create a test file with timestamp in name."""
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f"test_log_{timestamp}_auto.txt"
    
    with open(filename, 'w') as f:
        f.write(f"Test file created at {datetime.now().isoformat()}\n")
        f.write("This is a test for event-driven index updates.\n")
    
    return filename

def upload_to_s3(filename):
    """Upload file to S3."""
    s3 = boto3.client('s3', region_name=REGION)
    
    print(f"📤 Uploading {filename} to S3...")
    s3.upload_file(filename, BUCKET_NAME, filename)
    print(f"✅ Upload complete")
    
    return filename

def wait_for_processing(seconds=15):
    """Wait for event processing (increased to 15s for SQS delivery variance)."""
    print(f"⏳ Waiting {seconds} seconds for event processing...")
    for i in range(seconds):
        time.sleep(1)
        print(f"   {i+1}/{seconds}s", end='\r')
    print()

def check_worker_stats():
    """Check event worker statistics."""
    print(f"📊 Checking worker stats...")
    
    try:
        response = requests.get(f"{FLASK_URL}/api/event-worker/stats")
        data = response.json()
        
        if data.get('success'):
            stats = data['stats']
            print(f"✅ Worker Status:")
            print(f"   Running: {stats['running']}")
            print(f"   Events processed: {stats['events_processed']}")
            print(f"   Objects added: {stats['objects_added']}")
            print(f"   Objects deleted: {stats['objects_deleted']}")
            print(f"   Errors: {stats['errors']}")
            print(f"   Last poll: {stats['last_poll_time']}")
            return stats
        else:
            print(f"❌ Worker not running: {data.get('message')}")
            return None
    
    except Exception as e:
        print(f"❌ Error checking stats: {e}")
        return None

def search_for_file(filename):
    """Search for the uploaded file in index."""
    print(f"🔍 Searching for {filename} in index...")
    
    try:
        # Search latest 1 day
        response = requests.post(
            f"{FLASK_URL}/api/search/latest",
            json={"n_days": 1, "prefix": "test_log", "suffix": ".txt"}
        )
        
        data = response.json()
        
        if data.get('success'):
            results = data['results']
            found = any(r['key'] == filename for r in results)
            
            if found:
                print(f"✅ File found in index!")
                return True
            else:
                print(f"❌ File NOT found in index")
                print(f"   Total results: {len(results)}")
                return False
        else:
            print(f"❌ Search failed: {data.get('error')}")
            return False
    
    except Exception as e:
        print(f"❌ Error searching: {e}")
        return False

def delete_from_s3(filename):
    """Delete file from S3."""
    s3 = boto3.client('s3', region_name=REGION)
    
    print(f"🗑️  Deleting {filename} from S3...")
    s3.delete_object(Bucket=BUCKET_NAME, Key=filename)
    print(f"✅ Delete complete")

def cleanup_local_file(filename):
    """Delete local test file."""
    try:
        os.remove(filename)
        print(f"🧹 Cleaned up local file: {filename}")
    except:
        pass

def main():
    """Run full test suite."""
    print("\n" + "="*70)
    print("EVENT-DRIVEN INDEX UPDATE TEST")
    print("="*70 + "\n")
    
    # Check if Flask app is running
    try:
        response = requests.get(f"{FLASK_URL}/")
        print("✅ Flask app is running\n")
    except:
        print("❌ Flask app is not running. Start it first:")
        print("   python app_integrated.py")
        return
    
    # Test 1: Upload and Index
    print("TEST 1: Upload → Index")
    print("-" * 70)
    
    filename = create_test_file()
    upload_to_s3(filename)
    
    # Wait for event processing
    wait_for_processing(10)
    
    # Check stats
    stats_before = check_worker_stats()
    print()
    
    # Search for file
    found = search_for_file(filename)
    print()
    
    if not found:
        print("❌ TEST 1 FAILED: File not indexed")
        cleanup_local_file(filename)
        return
    
    print("✅ TEST 1 PASSED: File uploaded and indexed successfully\n")
    
    # Test 2: Delete and De-index
    print("TEST 2: Delete → De-index")
    print("-" * 70)
    
    delete_from_s3(filename)
    
    # Poll for deletion to be processed (up to 20 seconds)
    print("⏳ Polling for deletion event (up to 20 seconds)...")
    initial_deleted = stats_before['objects_deleted'] if stats_before else 0
    deletion_processed = False
    
    for attempt in range(10):  # 10 attempts x 2 seconds = 20 seconds max
        time.sleep(2)
        stats_after = check_worker_stats()
        if stats_after and stats_after['objects_deleted'] > initial_deleted:
            deletion_processed = True
            print(f"✅ Deletion event processed after ~{(attempt+1)*2} seconds")
            break
        print(f"   Attempt {attempt+1}/10 - Deleted: {stats_after['objects_deleted'] if stats_after else 0}", end='\r')
    
    print("\n")
    
    # Final stats check
    if not stats_after:
        stats_after = check_worker_stats()
    print()
    
    # Verify deletion
    if stats_after and stats_before:
        deleted_diff = stats_after['objects_deleted'] - stats_before['objects_deleted']
        if deleted_diff > 0:
            print(f"✅ TEST 2 PASSED: File deleted and de-indexed (diff: +{deleted_diff})\n")
        else:
            print(f"❌ TEST 2 FAILED: File not de-indexed (waited 20s)\n")
    
    # Cleanup
    cleanup_local_file(filename)
    
    print("="*70)
    print("TEST SUITE COMPLETE")
    print("="*70 + "\n")
    
    print("📋 Summary:")
    if stats_after:
        print(f"   Total events processed: {stats_after['events_processed']}")
        print(f"   Total objects added: {stats_after['objects_added']}")
        print(f"   Total objects deleted: {stats_after['objects_deleted']}")
        print(f"   Errors: {stats_after['errors']}")
    
    print("\n💡 To monitor in real-time, run:")
    print(f"   watch -n 1 'curl -s {FLASK_URL}/api/event-worker/stats | jq .stats'")

if __name__ == '__main__':
    main()
