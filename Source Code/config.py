"""
Configuration file for S3 Search Engine Flask Application
"""
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Base configuration."""
    
    # Flask settings
    SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    DEBUG = os.getenv('FLASK_DEBUG', 'False').lower() == 'true'
    
    # AWS S3 Configuration
    AWS_S3_BUCKET = os.getenv('AWS_S3_BUCKET', 'your-s3-bucket-name')
    AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', None)
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', None)
    S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', None)  # For S3-compatible services like S3 Ninja
    
    # S3 Prefix for filtering (e.g., 'log_2021' for 2021 files)
    S3_PREFIX = os.getenv('S3_PREFIX', '')
    
    # Local storage settings
    DATASET_DIR = os.getenv('DATASET_DIR', 'mock_s3_bucket')
    INDEX_FILE = os.getenv('INDEX_FILE', 's3_index.json')
    
    # Index cache settings (in seconds)
    INDEX_CACHE_TIMEOUT = int(os.getenv('INDEX_CACHE_TIMEOUT', 300))  # 5 minutes
    
    # Smart Refresh Settings - Incremental update optimization
    ENABLE_SMART_REFRESH = os.getenv('ENABLE_SMART_REFRESH', 'True').lower() == 'true'
    INCREMENTAL_REFRESH_ENABLED = os.getenv('INCREMENTAL_REFRESH_ENABLED', 'True').lower() == 'true'
    FULL_SCAN_INTERVAL = int(os.getenv('FULL_SCAN_INTERVAL', 86400))  # 24 hours (seconds)
    ENABLE_DELETION_DETECTION = os.getenv('ENABLE_DELETION_DETECTION', 'True').lower() == 'true'
    MAX_INCREMENTAL_AGE = int(os.getenv('MAX_INCREMENTAL_AGE', 300))  # 5 minutes
    
    # Large Dataset Optimization Settings
    USE_CHUNKED_INDEXING = os.getenv('USE_CHUNKED_INDEXING', 'False').lower() == 'true'
    INDEX_CHUNK_SIZE = int(os.getenv('INDEX_CHUNK_SIZE', 100_000))  # 100K objects per chunk
    USE_INDEX_COMPRESSION = os.getenv('USE_INDEX_COMPRESSION', 'False').lower() == 'true'
    
    # Use local mock data or AWS S3
    USE_MOCK_DATA = os.getenv('USE_MOCK_DATA', 'True').lower() == 'true'
    
    # Pagination settings
    RESULTS_PER_PAGE = int(os.getenv('RESULTS_PER_PAGE', 1000))
    
    # Export settings
    EXPORT_MAX_ROWS = int(os.getenv('EXPORT_MAX_ROWS', 10000))
    
    # Performance Optimization Settings (for 3M+ files)
    S3_MAX_WORKERS = int(os.getenv('S3_MAX_WORKERS', 150))  # Increased from 20 to 150 for async operations
    S3_MAX_POOL_CONNECTIONS = int(os.getenv('S3_MAX_POOL_CONNECTIONS', 250))  # Increased from 50 to 250
    LOCAL_INDEX_WORKERS = int(os.getenv('LOCAL_INDEX_WORKERS', 20))  # Parallel workers for local indexing
    
    # NEW: Optimized Service Settings (RECOMMENDED for 3M+ files)
    USE_OPTIMIZED_SERVICE = os.getenv('USE_OPTIMIZED_SERVICE', 'True').lower() == 'true'  # Use async S3 service
    USE_SQLITE_INDEX = os.getenv('USE_SQLITE_INDEX', 'False').lower() == 'true'  # Use SQLite instead of JSON
    SQLITE_DB_FILE = os.getenv('SQLITE_DB_FILE', 's3_index.db')  # SQLite database file
    
    # NEW: S3 Inventory Settings (for ultra-fast initial indexing)
    USE_S3_INVENTORY = os.getenv('USE_S3_INVENTORY', 'False').lower() == 'true'  # Use S3 Inventory for initial scan
    S3_INVENTORY_BUCKET = os.getenv('S3_INVENTORY_BUCKET', None)  # Bucket where inventory is stored (can be same)
    S3_INVENTORY_PREFIX = os.getenv('S3_INVENTORY_PREFIX', 'inventory/')  # Prefix where inventory files are stored
    S3_INVENTORY_FILE_PATH = os.getenv('S3_INVENTORY_FILE_PATH', None)  # Local path to downloaded inventory file
    
    # NEW: Event-Driven Real-Time Index Updates (S3 → SQS → Worker)
    ENABLE_EVENT_DRIVEN_UPDATES = os.getenv('ENABLE_EVENT_DRIVEN_UPDATES', 'False').lower() == 'true'
    SQS_QUEUE_URL = os.getenv('SQS_QUEUE_URL', None)  # SQS queue URL for S3 events
    SQS_POLL_INTERVAL = int(os.getenv('SQS_POLL_INTERVAL', 5))  # Seconds between polls (default: 5)
    SQS_MAX_MESSAGES = int(os.getenv('SQS_MAX_MESSAGES', 10))  # Max messages per poll (1-10)
    SQS_WAIT_TIME = int(os.getenv('SQS_WAIT_TIME', 20))  # Long polling wait time (0-20 seconds)
    SQS_VISIBILITY_TIMEOUT = int(os.getenv('SQS_VISIBILITY_TIMEOUT', 30))  # Message visibility timeout


class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    # USE_MOCK_DATA can be overridden by .env file


class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    USE_MOCK_DATA = False


class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    USE_MOCK_DATA = True


# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}


def get_config(env=None):
    """Get configuration based on environment."""
    if env is None:
        env = os.getenv('FLASK_ENV', 'development')
    return config.get(env, config['default'])
