"""
S3 Event Worker - Real-Time Index Updates via SQS
Polls SQS queue for S3 ObjectCreated/ObjectRemoved events and updates SQLite index
"""
import json
import time
import threading
from datetime import datetime, timezone
from typing import Optional, Dict
import boto3
from botocore.exceptions import ClientError

from index_db import IndexDB


class S3EventWorker:
    """
    Background worker that polls SQS for S3 events and updates SQLite index in real-time.
    Handles both ObjectCreated and ObjectRemoved events.
    """
    
    def __init__(self, queue_url: str, index_db: IndexDB,
                 poll_interval: int = 5,
                 max_messages: int = 10,
                 wait_time: int = 20,
                 visibility_timeout: int = 30,
                 aws_region: str = 'us-east-1'):
        """
        Initialize S3 event worker.
        
        Args:
            queue_url: SQS queue URL
            index_db: IndexDB instance for SQLite operations
            poll_interval: Seconds between polls (default: 5)
            max_messages: Max messages to fetch per poll (1-10)
            wait_time: Long polling wait time (0-20 seconds)
            visibility_timeout: Message visibility timeout (seconds)
            aws_region: AWS region
        """
        self.queue_url = queue_url
        self.index_db = index_db
        self.poll_interval = poll_interval
        self.max_messages = max_messages
        self.wait_time = wait_time
        self.visibility_timeout = visibility_timeout
        self.aws_region = aws_region
        
        # Create SQS client
        self.sqs = boto3.client('sqs', region_name=aws_region)
        
        # Control flags
        self.running = False
        self.thread = None
        
        # Statistics
        self.stats = {
            'events_processed': 0,
            'objects_added': 0,
            'objects_deleted': 0,
            'errors': 0,
            'last_poll_time': None
        }
    
    def start(self):
        """Start the worker thread."""
        if self.running:
            print("⚠️  Event worker already running")
            return
        
        self.running = True
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        print(f"✅ Event worker started")
    
    def stop(self):
        """Stop the worker thread gracefully."""
        if not self.running:
            return
        
        print("⏹️  Stopping event worker...")
        self.running = False
        
        if self.thread:
            self.thread.join(timeout=10)
        
        print("✅ Event worker stopped")
        self._print_stats()
    
    def _worker_loop(self):
        """Main worker loop - polls SQS and processes events."""
        print(f"🔄 Event worker loop started")
        
        while self.running:
            try:
                # Poll SQS for messages
                messages = self._poll_sqs()
                
                if messages:
                    self._process_messages(messages)
                
                # Sleep before next poll (only if no messages, long polling handles wait)
                if not messages:
                    time.sleep(self.poll_interval)
                
            except Exception as e:
                print(f"❌ Error in worker loop: {e}")
                self.stats['errors'] += 1
                time.sleep(self.poll_interval)
    
    def _poll_sqs(self) -> list:
        """
        Poll SQS queue for messages using long polling.
        
        Returns:
            List of SQS messages
        """
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time,
                VisibilityTimeout=self.visibility_timeout,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            
            self.stats['last_poll_time'] = datetime.now(timezone.utc).isoformat()
            
            messages = response.get('Messages', [])
            
            return messages
            
        except ClientError as e:
            print(f"❌ Error polling SQS: {e}")
            self.stats['errors'] += 1
            return []
    
    def _process_messages(self, messages: list):
        """
        Process SQS messages in batch.
        
        Args:
            messages: List of SQS messages
        """
        batch_added = 0
        batch_deleted = 0
        
        for message in messages:
            try:
                # Parse S3 event from message body
                body = json.loads(message['Body'])
                
                # Handle SNS-wrapped messages (S3 → SNS → SQS)
                if 'Message' in body:
                    event_data = json.loads(body['Message'])
                else:
                    event_data = body
                
                # Process S3 event records
                if 'Records' in event_data:
                    for record in event_data['Records']:
                        if self._process_s3_event(record):
                            if 'ObjectCreated' in record.get('eventName', ''):
                                batch_added += 1
                            elif 'ObjectRemoved' in record.get('eventName', ''):
                                batch_deleted += 1
                
                # Delete message from queue after successful processing
                self._delete_message(message)
                self.stats['events_processed'] += 1
                
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                self.stats['errors'] += 1
                # Don't delete message - it will become visible again for retry
        
        # Update stats
        self.stats['objects_added'] += batch_added
        self.stats['objects_deleted'] += batch_deleted
        
        if batch_added > 0 or batch_deleted > 0:
            print(f"✅ Processed: +{batch_added} added, -{batch_deleted} deleted")
    
    def _process_s3_event(self, record: Dict) -> bool:
        """
        Process a single S3 event record.
        
        Args:
            record: S3 event record
            
        Returns:
            True if processed successfully
        """
        try:
            event_name = record.get('eventName', '')
            s3_info = record.get('s3', {})
            bucket = s3_info.get('bucket', {}).get('name', '')
            obj = s3_info.get('object', {})
            key = obj.get('key', '')
            
            # URL decode key (S3 events encode special characters)
            from urllib.parse import unquote_plus
            key = unquote_plus(key)
            
            if not key:
                print("⚠️  Skipping event: no object key")
                return False
            
            # Handle ObjectCreated events
            if 'ObjectCreated' in event_name:
                # Extract metadata
                size = obj.get('size', 0)
                etag = obj.get('eTag', '')
                
                # Extract timestamp from event time
                event_time = record.get('eventTime', '')
                if event_time:
                    timestamp = datetime.fromisoformat(event_time.replace('Z', '+00:00')).isoformat()
                else:
                    timestamp = datetime.now(timezone.utc).isoformat()
                
                # Try to extract better timestamp from filename
                timestamp = self._extract_timestamp_from_key(key, timestamp)
                
                # Upsert to SQLite
                obj_data = {
                    'key': key,
                    'timestamp': timestamp,
                    'size': size,
                    'etag': etag
                }
                
                success = self.index_db.upsert_object(obj_data)
                
                if success:
                    print(f"✅ Indexed: {key}")
                
                return success
            
            # Handle ObjectRemoved events
            elif 'ObjectRemoved' in event_name:
                success = self.index_db.delete_object(key)
                
                if success:
                    print(f"🗑️  Deleted: {key}")
                
                return success
            
            else:
                print(f"⚠️  Unsupported event type: {event_name}")
                return False
                
        except Exception as e:
            print(f"❌ Error processing S3 event: {e}")
            return False
    
    def _extract_timestamp_from_key(self, key: str, fallback: str) -> str:
        """
        Extract timestamp from S3 key if it follows log_YYYY-MM-DD_HH-MM-SS pattern.
        
        Args:
            key: S3 object key
            fallback: Fallback timestamp
            
        Returns:
            ISO format timestamp
        """
        try:
            filename = key.split('/')[-1]
            
            if filename.startswith('log_') and filename.count('_') >= 2:
                parts = filename.split('_')
                if len(parts) >= 3:
                    date_part = parts[1]
                    time_part = parts[2].split('.')[0]
                    
                    timestamp_str = f"{date_part}_{time_part}"
                    parsed = datetime.strptime(timestamp_str, "%Y-%m-%d_%H-%M-%S")
                    parsed = parsed.replace(tzinfo=timezone.utc)
                    
                    return parsed.isoformat()
        except Exception:
            pass
        
        return fallback
    
    def _delete_message(self, message: Dict):
        """
        Delete message from SQS queue after successful processing.
        
        Args:
            message: SQS message
        """
        try:
            receipt_handle = message['ReceiptHandle']
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle
            )
        except Exception as e:
            print(f"❌ Error deleting message from SQS: {e}")
    
    def get_stats(self) -> Dict:
        """Get worker statistics."""
        return {
            'running': self.running,
            'events_processed': self.stats['events_processed'],
            'objects_added': self.stats['objects_added'],
            'objects_deleted': self.stats['objects_deleted'],
            'errors': self.stats['errors'],
            'last_poll_time': self.stats['last_poll_time']
        }
    
    def _print_stats(self):
        """Print worker statistics."""
        print(f"\n{'='*60}")
        print(f"EVENT WORKER STATISTICS")
        print(f"{'='*60}")
        print(f"Events processed: {self.stats['events_processed']}")
        print(f"Objects added: {self.stats['objects_added']}")
        print(f"Objects deleted: {self.stats['objects_deleted']}")
        print(f"Errors: {self.stats['errors']}")
        print(f"Last poll: {self.stats['last_poll_time']}")
        print(f"{'='*60}\n")


# Example usage
if __name__ == '__main__':
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    queue_url = os.getenv('SQS_QUEUE_URL')
    if not queue_url:
        print("❌ Error: SQS_QUEUE_URL not set in .env")
        exit(1)
    
    # Initialize SQLite index
    db = IndexDB('s3_index.db')
    
    # Create and start worker
    worker = S3EventWorker(
        queue_url=queue_url,
        index_db=db,
        poll_interval=5,
        max_messages=10,
        wait_time=20
    )
    
    try:
        worker.start()
        
        print("\n✅ Worker running. Press Ctrl+C to stop...\n")
        
        # Keep main thread alive
        while True:
            time.sleep(60)
            # Print stats every minute
            stats = worker.get_stats()
            print(f"\n📊 Stats: {stats['events_processed']} events | "
                  f"+{stats['objects_added']} added | -{stats['objects_deleted']} deleted | "
                  f"❌{stats['errors']} errors")
    
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    finally:
        worker.stop()
        db.close()
