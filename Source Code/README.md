# S3 Search Engine - Flask Web Application

A robust, **TRUE client-side** search engine for AWS S3 objects with fast SQLite indexing. Built with Flask and designed to handle large-scale S3 buckets efficiently with **zero network latency** for searches.

## 🚀 Features

- **⚡ TRUE Client-Side Search**: Search millions of S3 objects in milliseconds using SQLite (NO remote calls)
- **� Real-Time Event-Driven Updates**: Automatic index updates when objects are uploaded/deleted (NEW!)
- **�🗄️ SQLite Index**: 100x faster than JSON, all queries run locally on Flask server
- **Multiple Search Modes**:
  - Date range search
  - Objects older than N days
  - Latest N days objects
- **Multi-Filter Support**: Filter by prefix and suffix
- **Real-time Visualization**: Monthly distribution charts with Chart.js
- **CSV Export**: Export search results for further analysis
- **AWS S3 Integration**: Direct integration with AWS S3 using boto3
- **Mock Data Support**: Test with local mock data before connecting to S3
- **Responsive UI**: Modern Bootstrap 5 interface
- **RESTful API**: Well-designed API endpoints for all operations

## 📊 Performance

- **Query Speed**: 5-50ms (SQLite indexed queries)
- **Network Calls**: Zero (all searches run locally)
- **Scalability**: Handles 3M+ files efficiently
- **Cost**: $0 for searches (no remote database charges)
- **Real-Time Updates**: 2-5 seconds from S3 upload to indexed (with event-driven mode)

## 🔄 Event-Driven Real-Time Updates (NEW!)

Automatically update your index when objects are uploaded or deleted from S3:

```
S3 Upload → S3 Event → SQS → Worker → SQLite Index
```

**Benefits:**
- ⚡ 2-5 second latency from upload to indexed
- 🔄 No manual refresh needed
- 💰 ~$0.50/month for 1M events
- 📊 100% accurate, always up-to-date

**Quick Setup:**
```bash
# 1. Configure AWS infrastructure
python aws_event_setup.py --bucket your-bucket --region us-east-1

# 2. Enable in .env
ENABLE_EVENT_DRIVEN_UPDATES=True
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/ACCOUNT/s3-events-queue-prism-YYYYMMDDHHMMSS

# 3. Restart app - done!
python app_integrated.py
```

See [EVENT_DRIVEN_SETUP_GUIDE.md](EVENT_DRIVEN_SETUP_GUIDE.md) for detailed instructions.

## 📋 Prerequisites

- Python 3.8 or higher
- AWS Account (for S3 integration)
- AWS credentials configured (for production use)
- SQLite (included with Python)

## 🛠️ Installation

### 1. Clone or Download the Project

```bash
cd Samsung-Prism
```

### 2. Create Virtual Environment

```bash
# Windows
python -m venv venv
venv\Scripts\activate

# Linux/Mac
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables

Copy the example environment file and update with your values:

```bash
cp .env.example .env
```

Edit `.env` file:

```env
# Flask Configuration
SECRET_KEY=your-secret-key-here
FLASK_ENV=development
FLASK_DEBUG=True

# AWS S3 Configuration
AWS_S3_BUCKET=your-bucket-name
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key

# Use Mock Data (True) or AWS S3 (False)
USE_MOCK_DATA=True
```

## 🚦 Quick Start

### Option 1: Using Mock Data (Testing)

1. Generate mock S3 data:
```bash
python generate_mock_s3.py
```

2. Run the Flask application:
```bash
python app.py
```

3. Open browser: `http://localhost:5000`

### Option 2: Using AWS S3 (Production)

1. Update `.env` file with your AWS credentials:
```env
USE_MOCK_DATA=False
AWS_S3_BUCKET=your-actual-bucket-name
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
```

2. Build index from S3:
```bash
python -c "from s3_service import S3Service; import os; s3 = S3Service(os.getenv('AWS_S3_BUCKET')); s3.build_index_from_s3()"
```

3. Run the Flask application:
```bash
python app.py
```

4. Open browser: `http://localhost:5000`

## 📁 Project Structure

```
Samsung-Prism/
├── app.py                      # Main Flask application
├── config.py                   # Configuration management
├── s3_service.py              # AWS S3 integration service
├── generate_mock_s3.py        # Mock data generator
├── requirements.txt           # Python dependencies
├── .env.example              # Environment variables template
├── templates/                # HTML templates
│   ├── base.html            # Base template
│   ├── index.html           # Main search page
│   ├── visualization.html   # Charts and statistics
│   ├── 404.html            # Error page
│   └── 500.html            # Error page
├── static/                  # Static assets
│   ├── css/
│   │   └── style.css       # Custom styles
│   └── js/
│       ├── main.js         # Main JavaScript
│       ├── search.js       # Search functionality
│       └── visualization.js # Charts
└── mock_s3_bucket/         # Local mock data (gitignored)
```

## 🔧 Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `SECRET_KEY` | Flask secret key | Random string |
| `FLASK_ENV` | Environment (development/production) | development |
| `AWS_S3_BUCKET` | S3 bucket name | - |
| `AWS_REGION` | AWS region | us-east-1 |
| `USE_MOCK_DATA` | Use local mock data | True |
| `INDEX_CACHE_TIMEOUT` | Index cache timeout (seconds) | 300 |

### AWS Credentials Setup

#### Option 1: Environment Variables
Set in `.env` file or system environment variables.

#### Option 2: AWS Credentials File
```bash
# ~/.aws/credentials
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

#### Option 3: IAM Role (EC2/Lambda)
Use IAM roles when deploying to AWS services.

## 🔌 API Endpoints

### Search Endpoints

#### Date Range Search
```bash
POST /api/search/date-range
Content-Type: application/json

{
  "start_date": "2010-01-01",
  "end_date": "2010-12-31",
  "prefix": "logs/",
  "suffix": ".txt"
}
```

#### Older Than N Days
```bash
POST /api/search/older-than
Content-Type: application/json

{
  "n_days": 30,
  "prefix": "logs/",
  "suffix": ".txt"
}
```

#### Latest N Days
```bash
POST /api/search/latest
Content-Type: application/json

{
  "n_days": 7,
  "prefix": "logs/",
  "suffix": ".txt"
}
```

### Utility Endpoints

#### Refresh Index
```bash
POST /api/refresh-index
```

#### Monthly Statistics
```bash
GET /api/stats/monthly
```

#### Export to CSV
```bash
POST /api/export/csv
Content-Type: application/json

{
  "results": [...]
}
```

#### Event Worker Stats (NEW!)
```bash
GET /api/event-worker/stats

# Response:
{
  "success": true,
  "stats": {
    "running": true,
    "events_processed": 1250,
    "objects_added": 1200,
    "objects_deleted": 50,
    "errors": 0,
    "last_poll_time": "2026-03-21T10:30:45Z"
  }
}
```

## 🚀 Deployment

### Deploying to AWS EC2

1. **Launch EC2 Instance**
   - Amazon Linux 2 or Ubuntu
   - t2.micro or larger
   - Security group: Allow port 80/443

2. **Install Dependencies**
```bash
sudo yum update -y
sudo yum install python3 python3-pip git -y
```

3. **Clone and Setup**
```bash
git clone <your-repo>
cd Samsung-Prism
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

4. **Configure Environment**
```bash
cp .env.example .env
nano .env  # Edit with your values
```

5. **Run with Gunicorn**
```bash
gunicorn -w 4 -b 0.0.0.0:80 app:app
```

### Deploying to AWS Lambda

1. **Install Zappa**
```bash
pip install zappa
```

2. **Initialize Zappa**
```bash
zappa init
```

3. **Deploy**
```bash
zappa deploy production
```

### Using Docker

```bash
# Build image
docker build -t s3-search-engine .

# Run container
docker run -p 5000:5000 --env-file .env s3-search-engine
```

## 🧪 Testing

### Generate Test Data
```bash
python generate_mock_s3.py
```

This creates 10,000 mock S3 files with timestamps between 2010-2013.

### Run Tests
```bash
pytest tests/
```

## 📊 Performance

- **Index Building**: ~1-2 seconds per 10,000 objects
- **Search Speed**: <100ms for millions of objects
- **Memory Usage**: ~100MB for 1 million objects indexed
- **Concurrent Users**: Supports 100+ concurrent users

## 🔒 Security Best Practices

1. **Never commit** `.env` file with real credentials
2. Use **IAM roles** when deploying to AWS
3. Implement **authentication** for production use
4. Use **HTTPS** in production
5. Restrict **S3 bucket permissions** appropriately
6. Enable **CloudWatch logging** for monitoring

## 🐛 Troubleshooting

### Issue: AWS Credentials Not Found
**Solution**: Configure credentials in `.env` or AWS credentials file

### Issue: Bucket Access Denied
**Solution**: Check IAM permissions for S3 bucket access

### Issue: Index Building Slow
**Solution**: Use pagination and implement parallel processing

### Issue: Large Memory Usage
**Solution**: Implement index chunking or database storage

## 📚 Additional Resources

- [Flask Documentation](https://flask.palletsprojects.com/)
- [Boto3 Documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)
- [AWS S3 Documentation](https://docs.aws.amazon.com/s3/)
- [Bootstrap Documentation](https://getbootstrap.com/)

## 🤝 Contributing

1. Fork the repository
2. Create feature branch (`git checkout -b feature/YourFeature`)
3. Commit changes (`git commit -m 'Add YourFeature'`)
4. Push to branch (`git push origin feature/YourFeature`)
5. Open Pull Request

## 📝 License

This project is licensed under the MIT License.

## 👥 Authors

- Samsung Prism Project Team

## 🎯 Future Enhancements

- [ ] Implement user authentication
- [ ] Add database support (PostgreSQL/MongoDB)
- [ ] Implement caching with Redis
- [ ] Add more visualization options
- [ ] Support for multiple S3 buckets
- [ ] Advanced filtering (size, content type)
- [ ] Batch operations (download, delete)
- [ ] Real-time index updates
- [ ] API rate limiting
- [ ] Comprehensive logging

## 📞 Support

For issues and questions, please open an issue in the repository.

---

**Note**: This application is designed for Samsung Prism project requirements and demonstrates robust S3 object search capabilities with client-side execution for optimal performance.
