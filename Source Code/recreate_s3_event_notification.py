"""
Recreate S3 Event Notification
Deletes the old configuration and creates a fresh one
"""
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

BUCKET_NAME = os.getenv('AWS_S3_BUCKET', 'samsung-prism-logs')
REGION = os.getenv('AWS_REGION', 'us-east-1')
QUEUE_URL = os.getenv('SQS_QUEUE_URL')

print("\n" + "="*80)
print("RECREATE S3 EVENT NOTIFICATION")
print("="*80)

# Step 1: Get Queue ARN from Queue URL
print("\n1️⃣  Getting SQS Queue ARN")
print("-"*80)

sqs = boto3.client('sqs', region_name=REGION)
try:
    attrs = sqs.get_queue_attributes(
        QueueUrl=QUEUE_URL,
        AttributeNames=['QueueArn']
    )
    QUEUE_ARN = attrs['Attributes']['QueueArn']
    print(f"✅ Queue ARN: {QUEUE_ARN}")
except Exception as e:
    print(f"❌ Error getting queue ARN: {e}")
    exit(1)

# Step 2: Get current event notification configuration
print("\n2️⃣  Checking Current Event Notifications")
print("-"*80)

s3 = boto3.client('s3', region_name=REGION)

try:
    config = s3.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
    
    # Remove metadata
    config.pop('ResponseMetadata', None)
    
    if 'QueueConfigurations' in config:
        print(f"✅ Found {len(config['QueueConfigurations'])} queue configuration(s):")
        for i, qc in enumerate(config['QueueConfigurations']):
            print(f"   [{i+1}] ID: {qc.get('Id', 'N/A')}")
            print(f"       Queue: {qc.get('QueueArn', 'N/A')}")
            print(f"       Events: {qc.get('Events', [])}")
    else:
        print(f"⚠️  No queue configurations found")
        
except Exception as e:
    print(f"❌ Error getting notification config: {e}")
    exit(1)

# Step 3: Delete ALL existing event notification configurations
print("\n3️⃣  Deleting All Existing Event Notifications")
print("-"*80)

try:
    # Put an empty configuration to clear all notifications
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration={}
    )
    print(f"✅ Deleted all event notification configurations")
    
except Exception as e:
    print(f"❌ Error deleting notifications: {e}")
    exit(1)

# Step 4: Wait a moment for propagation
print("\n4️⃣  Waiting for AWS Propagation (10 seconds)")
print("-"*80)

import time
for i in range(10):
    time.sleep(1)
    print(f"   {i+1}/10s", end='\r')
print()
print(f"✅ Wait complete")

# Step 5: Create NEW event notification
print("\n5️⃣  Creating New Event Notification")
print("-"*80)

new_config = {
    'QueueConfigurations': [
        {
            'Id': 's3-index-events-v3',  # New version number
            'QueueArn': QUEUE_ARN,
            'Events': [
                's3:ObjectCreated:*',
                's3:ObjectRemoved:*'
            ]
            # No Filter key = match ALL objects
        }
    ]
}

try:
    s3.put_bucket_notification_configuration(
        Bucket=BUCKET_NAME,
        NotificationConfiguration=new_config
    )
    print(f"✅ Created new event notification!")
    print(f"   ID: s3-index-events-v3")
    print(f"   Queue: {QUEUE_ARN}")
    print(f"   Events: s3:ObjectCreated:*, s3:ObjectRemoved:*")
    print(f"   Filter: (none - matches all objects)")
    
except Exception as e:
    print(f"❌ Error creating notification: {e}")
    exit(1)

# Step 6: Verify the new configuration
print("\n6️⃣  Verifying New Configuration")
print("-"*80)

try:
    verify_config = s3.get_bucket_notification_configuration(Bucket=BUCKET_NAME)
    verify_config.pop('ResponseMetadata', None)
    
    if 'QueueConfigurations' in verify_config:
        print(f"✅ Verified: {len(verify_config['QueueConfigurations'])} queue configuration(s)")
        for qc in verify_config['QueueConfigurations']:
            print(f"   ✓ ID: {qc.get('Id')}")
            print(f"   ✓ Events: {', '.join(qc.get('Events', []))}")
    else:
        print(f"⚠️  Warning: No configurations found (this shouldn't happen!)")
        
except Exception as e:
    print(f"❌ Error verifying configuration: {e}")

print("\n" + "="*80)
print("RECREATION COMPLETE!")
print("="*80)
print("\nℹ️  NOTE: S3 Event Notifications may take 1-5 minutes to fully propagate.")
print("   Test with: python ultimate_event_test.py")
print("="*80 + "\n")
