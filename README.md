# AWS_S3_Search_engine

Production-oriented Flask application for high-volume S3 object indexing and search with SQLite-backed queries and event-driven updates.

This project supports both:

- Scheduled or on-demand indexing from S3 listings (full scan or smart refresh)
- Real-time event-driven indexing (S3 notifications to SQS to worker to SQLite)

The repository is optimized for large buckets (millions of objects) and includes utilities for dataset generation, migration, upload, and operational verification.

## Tech Stack and Versions

### Backend and Runtime

- Python: 3.12
- Flask: 3.0.0
- Werkzeug: 3.0.1
- Gunicorn: 21.2.0
- python-dotenv: 1.0.0
- SQLite: built-in with Python runtime (used as local index database)

### AWS and Async SDKs

- boto3: >=1.34.0
- botocore: >=1.34.0
- aioboto3: >=15.0.0 (optional optimized path)
- aiohttp: >=3.13.0 (async transport dependency)

### Frontend Libraries

- Bootstrap: 5.3.0
- Bootstrap Icons: 1.10.0
- Chart.js: 4.4.0

### Dev and Quality Tooling

- pytest: 7.4.3
- pytest-flask: 1.3.0
- requests: 2.31.0
- flake8: 7.0.0
- black: 23.12.1
- mypy: 1.7.1

## 1. Core Capabilities

- Millisecond-to-low-second local query performance using SQLite indexes
- Search modes: date range, older-than, latest-N-days
- Prefix and suffix filtering
- Monthly distribution stats and CSV export
- Real-time index updates from S3 object events via SQS worker
- Batch S3 object operations: copy, move, delete
- Modular web and worker services with shared SQLite-backed index

## 2. High-Level Architecture

1. User/UI calls Flask API endpoints.
2. Flask query layer reads from local index backend (SQLite preferred).
3. For indexing updates:
   - Full/smart refresh path: S3 listing through service layer.
   - Event-driven path: S3 event notification to SQS, consumed by worker, persisted in SQLite.

Event-driven data path:

S3 ObjectCreated or ObjectRemoved -> S3 Event Notification -> SQS Queue -> prism-worker -> SQLite (objects table) -> prism-web search endpoints

## 3. Application Flow

### Search and query flow

1. Client sends search request to Flask endpoint.
2. Flask resolves index backend through get_index in app_integrated.py.
3. If USE_SQLITE_INDEX=True, search is executed in SQL via index_db.py.
4. Results are returned as JSON for UI rendering and optional CSV export.

### Event-driven indexing flow

1. Object is uploaded/deleted in S3.
2. S3 publishes event to configured SQS queue.
3. prism-worker (s3_event_worker_runner.py) polls queue.
4. s3_event_worker.py parses records and upserts/deletes in SQLite.
5. New searches immediately reflect updated index.

## 4. Prerequisites

- Python 3.12 recommended
- AWS account and S3 bucket access
- AWS IAM permissions for:
  - s3:ListBucket
  - s3:GetObject
  - s3:PutBucketNotification
  - sqs:GetQueueAttributes
  - sqs:SetQueueAttributes
  - sqs:ReceiveMessage
  - sqs:DeleteMessage
  - sqs:SendMessage

## 5. Clone and Install

### 5.1 Clone repository

Replace the URL with your Git remote:

```bash
git clone <your-repo-url>
cd Prism-Code
```

### 5.2 Create virtual environment

Windows PowerShell:

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Linux/macOS:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 5.3 Install dependencies

```bash
pip install --upgrade pip
pip install -r requirements.txt
```

## 6. Configuration

Copy template and edit:

```powershell
Copy-Item .env.example .env
```

Minimum required values for AWS mode:

- AWS_S3_BUCKET
- AWS_REGION
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- USE_MOCK_DATA=False
- USE_SQLITE_INDEX=True

For event-driven updates:

- ENABLE_EVENT_DRIVEN_UPDATES=True (worker service)
- SQS_QUEUE_URL=<your queue URL>

Security note:

- Do not commit real keys in .env or .env.example.
- Rotate credentials immediately if exposed.

## 7. Local Runbook (Recommended)

Run the application and worker as separate local processes.

### 7.1 Start Flask app

```bash
python app_integrated.py
```

### 7.2 Start SQS worker (separate terminal)

```bash
python s3_event_worker_runner.py
```

### 7.3 Open app

- http://localhost:5000

Notes:

- Use the same .env for web and worker processes.
- Keep ENABLE_EVENT_DRIVEN_UPDATES=False in web process when worker is started separately.
- Set ENABLE_EVENT_DRIVEN_UPDATES=True for the dedicated worker process.

## 8. Event Notification Setup and Validation

### 8.1 Recreate S3 event notification mapping

Use this whenever queue URL changes:

```bash
python recreate_s3_event_notification.py
```

### 8.2 Verify upload to index propagation

Current test script validates upload to index stage only:

```bash
python test_event_updates.py
```

Expected outcome includes:

- Upload success to S3
- File found in /api/search/latest response

Operational best practice:

- Use a dedicated SQS queue per environment to avoid cross-consumer message drain.

## 9. Alternative Local Run Steps

### 9.1 Start Flask app

```bash
python app_integrated.py
```

### 9.2 Optional: start worker in separate terminal

```bash
python s3_event_worker_runner.py
```

### 9.3 Local URL

- http://localhost:5000 (default Flask)

## 10. API Overview (Current Routes)

### UI and utility routes

- GET /
- GET /visualization
- GET /api/stats/monthly
- POST /api/refresh-index
- POST /api/export/csv
- GET /api/s3/test-connection
- GET /api/event-worker/stats

### Search routes

- POST /api/search/date-range
- POST /api/search/older-than
- POST /api/search/latest

### Object operation routes

- POST /api/objects/delete
- POST /api/objects/copy
- POST /api/objects/move
- GET /api/buckets/validate

## 11. Python Module Reference (Every .py File in Workspace)

1. app_integrated.py
- Main Flask application entrypoint.
- Defines all API routes, search behavior, index backend selection, and optional in-process event worker startup.

2. config.py
- Central configuration model loaded from .env.
- Defines environment classes (development, production, testing) and feature flags.

3. s3_service.py
- Synchronous boto3 service for full indexing and incremental refresh.
- Handles S3 listing, timestamp extraction, metadata persistence, and search helper operations.

4. s3_service_optimized.py
- Async aioboto3 implementation for high-concurrency indexing/refresh scenarios.
- Optimized for larger datasets and connection pools.

5. index_db.py
- SQLite abstraction layer.
- Creates schema/indexes, upsert/delete operations, and optimized SQL query methods.

6. s3_event_worker.py
- SQS polling worker implementation.
- Parses S3 event payloads and writes object create/delete changes into SQLite.

7. s3_event_worker_runner.py
- Standalone worker process launcher.
- Validates config and runs S3EventWorker continuously.

8. recreate_s3_event_notification.py
- Ops utility to remove and recreate S3 bucket notification configuration to SQS.
- Useful when queue URL/ARN changed or event delivery is inconsistent.

9. test_event_updates.py
- Verification script for event-driven pipeline.
- Current behavior validates upload-to-index stage and skips delete/de-index stage.

10. import_json_to_sqlite.py
- One-time migration utility from s3_index.json to s3_index.db.
- Helpful when moving from JSON index mode to SQLite mode.

11. generate_mockdata_s3.py
- High-volume mock log generator.
- Creates local dataset with configurable file count/date range and hierarchical folder structure.

12. upload_to_s3.py
- Parallel uploader from local mock dataset to S3/S3-compatible endpoint.
- Uses thread pool and connection pooling for throughput.

13. filecount_S3.py
- Simple object count utility for S3 bucket via pagination.

## 12. Recommended Operational Commands

### Start Flask app

```bash
python app_integrated.py
```

### Start worker

```bash
python s3_event_worker_runner.py
```

### Run event pipeline test

```bash
python test_event_updates.py
```

### Recreate S3 notification

```bash
python recreate_s3_event_notification.py
```

### Count objects in S3

```bash
python filecount_S3.py
```

## 13. Troubleshooting Guide

### Symptom: Upload succeeds but object not searchable

Checklist:

1. Confirm object exists in S3 bucket.
2. Confirm S3 bucket notification targets expected queue.
3. Confirm worker logs show message consumption and indexing.
4. Confirm web and worker use the same SQLITE_DB_FILE path.
5. Use dedicated queue per environment to avoid competing consumers.

### Symptom: /api/event-worker/stats returns worker not running

Explanation:

- Web app and worker are separate local processes.
- Ensure worker process is started and running continuously.
- Check worker terminal output for authoritative runtime status.

### Symptom: Slow or stale search responses

1. Ensure USE_SQLITE_INDEX=True.
2. Verify SQLITE_DB_FILE points to persistent path.
3. Check INDEX_CACHE_TIMEOUT and refresh strategy settings.

## 14. Production Readiness Notes

- Run with least-privilege IAM and key rotation.
- Isolate SQS queues per environment (dev/stage/prod).
- Persist /app/data volume and back up SQLite file as needed.
- Add monitoring for queue depth, worker errors, and API latency.
- Keep secrets out of repository and CI logs.

## 15. Future Contact for reference:
Contact for s3_index.db file. It cannot be uploaded to repository because of large size around 1 Gb.
Mohana Krishnan G - mohanakrishnang2003@gmail.com


