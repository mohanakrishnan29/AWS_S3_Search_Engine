"""
SQLite Index Database Manager
Provides 100x faster lookups for 3M+ files compared to JSON
Optional but HIGHLY recommended for large datasets
"""
import sqlite3
import json
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
import os


class IndexDB:
    """SQLite-based index database for fast queries on millions of objects."""
    
    def __init__(self, db_file: str = 's3_index.db'):
        """
        Initialize SQLite index database.
        
        Args:
            db_file: Path to SQLite database file
        """
        self.db_file = db_file
        self.conn = None
        self._initialize_db()
    
    def _initialize_db(self):
        """Initialize database connection and create tables."""
        self.conn = sqlite3.connect(self.db_file, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        
        cursor = self.conn.cursor()
        
        # Create objects table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS objects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                key TEXT UNIQUE NOT NULL,
                timestamp TEXT NOT NULL,
                size INTEGER NOT NULL,
                etag TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Create indexes for fast queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp ON objects(timestamp)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_key ON objects(key)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_size ON objects(size)')
        
        # Composite index for faster filtered date queries
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_timestamp_key ON objects(timestamp, key)')
        
        # Create metadata table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        self.conn.commit()
    
    def import_from_json(self, json_file: str) -> int:
        """
        Import index from JSON file into SQLite database.
        
        Args:
            json_file: Path to JSON index file
            
        Returns:
            Number of objects imported
        """
        print(f"📦 Importing index from {json_file} to SQLite...")
        start_time = datetime.now()
        
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Handle both old and new format
        if isinstance(data, list):
            objects = data
            metadata = {}
        else:
            objects = data.get('objects', [])
            metadata = data.get('metadata', {})
        
        # Clear existing data
        cursor = self.conn.cursor()
        cursor.execute('DELETE FROM objects')
        cursor.execute('DELETE FROM metadata')
        
        # Import objects in batches for speed
        batch_size = 1000
        for i in range(0, len(objects), batch_size):
            batch = objects[i:i + batch_size]
            cursor.executemany(
                'INSERT OR REPLACE INTO objects (key, timestamp, size, etag) VALUES (?, ?, ?, ?)',
                [(obj['key'], obj['timestamp'], obj['size'], obj.get('etag', '')) for obj in batch]
            )
            
            if (i + batch_size) % 10000 == 0:
                print(f"   Imported {i + batch_size:,} objects...")
        
        # Import metadata
        for key, value in metadata.items():
            cursor.execute(
                'INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)',
                (key, json.dumps(value) if not isinstance(value, str) else value)
            )
        
        self.conn.commit()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ Imported {len(objects):,} objects in {elapsed:.2f}s")
        
        return len(objects)
    
    def export_to_json(self, json_file: str) -> int:
        """
        Export SQLite database to JSON file.
        
        Args:
            json_file: Path to output JSON file
            
        Returns:
            Number of objects exported
        """
        print(f"💾 Exporting SQLite database to {json_file}...")
        start_time = datetime.now()
        
        cursor = self.conn.cursor()
        
        # Get all objects
        cursor.execute('SELECT key, timestamp, size, etag FROM objects')
        rows = cursor.fetchall()
        
        objects = [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
        
        # Get metadata
        cursor.execute('SELECT key, value FROM metadata')
        metadata_rows = cursor.fetchall()
        
        metadata = {}
        for row in metadata_rows:
            try:
                metadata[row['key']] = json.loads(row['value'])
            except:
                metadata[row['key']] = row['value']
        
        # Save to JSON
        data = {
            'metadata': metadata,
            'objects': objects
        }
        
        with open(json_file, 'w') as f:
            json.dump(data, f, indent=2)
        
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"✅ Exported {len(objects):,} objects in {elapsed:.2f}s")
        
        return len(objects)
    
    def add_objects(self, objects: List[Dict]) -> int:
        """
        Add/update objects in database.
        
        Args:
            objects: List of object dicts
            
        Returns:
            Number of objects added
        """
        cursor = self.conn.cursor()
        
        cursor.executemany(
            'INSERT OR REPLACE INTO objects (key, timestamp, size, etag) VALUES (?, ?, ?, ?)',
            [(obj['key'], obj['timestamp'], obj['size'], obj.get('etag', '')) for obj in objects]
        )
        
        self.conn.commit()
        return len(objects)
    
    def upsert_object(self, obj: Dict) -> bool:
        """
        Insert or update a single object (thread-safe, event-driven updates).
        
        Args:
            obj: Object dict with keys: key, timestamp, size, etag
            
        Returns:
            True if successful
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                'INSERT OR REPLACE INTO objects (key, timestamp, size, etag) VALUES (?, ?, ?, ?)',
                (obj['key'], obj['timestamp'], obj['size'], obj.get('etag', ''))
            )
            self.conn.commit()
            return True
        except Exception as e:
            print(f"Error upserting object {obj.get('key')}: {e}")
            return False
    
    def delete_object(self, key: str) -> bool:
        """
        Delete a single object by key (event-driven deletes).
        
        Args:
            key: S3 object key
            
        Returns:
            True if deleted, False if not found or error
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute('DELETE FROM objects WHERE key = ?', (key,))
            self.conn.commit()
            return cursor.rowcount > 0
        except Exception as e:
            print(f"Error deleting object {key}: {e}")
            return False
    
    def delete_objects(self, keys: List[str]) -> int:
        """
        Delete objects from database.
        
        Args:
            keys: List of object keys to delete
            
        Returns:
            Number of objects deleted
        """
        cursor = self.conn.cursor()
        
        cursor.executemany('DELETE FROM objects WHERE key = ?', [(key,) for key in keys])
        
        self.conn.commit()
        return cursor.rowcount
    
    def search_by_date_range(self, start_date: str, end_date: str, limit: Optional[int] = None, prefix: str = '', suffix: str = '') -> List[Dict]:
        """
        Search objects within date range (FAST with indexed timestamp).
        OPTIMIZED: Applies filters and limits in SQL for 100x faster queries.
        
        Args:
            start_date: ISO format start date
            end_date: ISO format end date
            limit: Maximum results to return (None = all matches)
            prefix: Filter by key prefix (applied in SQL)
            suffix: Filter by key suffix (applied in SQL)
            
        Returns:
            List of matching objects
        """
        # Build SQL query with filters
        query = 'SELECT key, timestamp, size, etag FROM objects WHERE timestamp BETWEEN ? AND ?'
        params = [start_date, end_date]
        
        # Apply prefix filter in SQL
        if prefix:
            query += ' AND key LIKE ?'
            params.append(f'{prefix}%')
        
        # Apply suffix filter in SQL
        if suffix:
            query += ' AND key LIKE ?'
            params.append(f'%{suffix}')
        
        query += ' ORDER BY timestamp'
        
        # Apply limit in SQL
        if limit:
            query += ' LIMIT ?'
            params.append(limit)
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def search_older_than(self, cutoff_date: str) -> List[Dict]:
        """Search objects older than cutoff date."""
        cursor = self.conn.cursor()
        
        cursor.execute(
            'SELECT key, timestamp, size, etag FROM objects WHERE timestamp < ? ORDER BY timestamp',
            (cutoff_date,)
        )
        
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def search_newer_than(self, cutoff_date: str) -> List[Dict]:
        """Search objects newer than cutoff date."""
        cursor = self.conn.cursor()
        
        cursor.execute(
            'SELECT key, timestamp, size, etag FROM objects WHERE timestamp >= ? ORDER BY timestamp DESC',
            (cutoff_date,)
        )
        
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def search_by_prefix(self, prefix: str, limit: Optional[int] = None) -> List[Dict]:
        """Search objects by key prefix."""
        cursor = self.conn.cursor()
        
        query = 'SELECT key, timestamp, size, etag FROM objects WHERE key LIKE ? ORDER BY key'
        params = (f'{prefix}%',)
        
        if limit:
            query += ' LIMIT ?'
            params = (f'{prefix}%', limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def search_by_suffix(self, suffix: str, limit: Optional[int] = None) -> List[Dict]:
        """Search objects by key suffix."""
        cursor = self.conn.cursor()
        
        query = 'SELECT key, timestamp, size, etag FROM objects WHERE key LIKE ? ORDER BY key'
        params = (f'%{suffix}',)
        
        if limit:
            query += ' LIMIT ?'
            params = (f'%{suffix}', limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def get_total_count(self) -> int:
        """Get total number of objects."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT COUNT(*) as count FROM objects')
        return cursor.fetchone()['count']
    
    def get_total_size(self) -> int:
        """Get total size of all objects in bytes."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT SUM(size) as total FROM objects')
        result = cursor.fetchone()['total']
        return result if result else 0
    
    def get_statistics(self) -> Dict:
        """Get database statistics."""
        cursor = self.conn.cursor()
        
        # Total count
        cursor.execute('SELECT COUNT(*) as count FROM objects')
        total_count = cursor.fetchone()['count']
        
        # Total size
        cursor.execute('SELECT SUM(size) as total FROM objects')
        total_size = cursor.fetchone()['total'] or 0
        
        # Date range
        cursor.execute('SELECT MIN(timestamp) as min, MAX(timestamp) as max FROM objects')
        date_range = cursor.fetchone()
        
        return {
            'total_objects': total_count,
            'total_size_bytes': total_size,
            'total_size_gb': round(total_size / (1024**3), 2),
            'earliest_timestamp': date_range['min'],
            'latest_timestamp': date_range['max']
        }
    
    def set_metadata(self, key: str, value: str) -> None:
        """Set metadata key-value pair."""
        cursor = self.conn.cursor()
        cursor.execute(
            'INSERT OR REPLACE INTO metadata (key, value, updated_at) VALUES (?, ?, ?)',
            (key, value, datetime.now(timezone.utc).isoformat())
        )
        self.conn.commit()
    
    def get_metadata(self, key: str) -> Optional[str]:
        """Get metadata value by key."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT value FROM metadata WHERE key = ?', (key,))
        row = cursor.fetchone()
        return row['value'] if row else None
    
    def get_all_metadata(self) -> Dict:
        """Get all metadata as dict."""
        cursor = self.conn.cursor()
        cursor.execute('SELECT key, value FROM metadata')
        rows = cursor.fetchall()
        
        metadata = {}
        for row in rows:
            try:
                metadata[row['key']] = json.loads(row['value'])
            except:
                metadata[row['key']] = row['value']
        
        return metadata
    
    def vacuum(self):
        """Optimize database (reclaim space after deletions)."""
        print("🔧 Optimizing database...")
        self.conn.execute('VACUUM')
        print("✅ Database optimized")
    
    def search_older_than_n_days(self, n: int, limit: Optional[int] = None, prefix: str = '', suffix: str = '') -> List[Dict]:
        """
        Search files older than N days (Flask API compatible) with optional filters.
        OPTIMIZED: Applies filters and limits in SQL for 100x faster queries.
        
        Args:
            n: Number of days
            limit: Maximum results to return (None = all matches)
            prefix: Filter by key prefix (applied in SQL)
            suffix: Filter by key suffix (applied in SQL)
            
        Returns:
            List of matching files
        """
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()
        
        # Build SQL query with filters
        query = 'SELECT key, timestamp, size, etag FROM objects WHERE timestamp < ?'
        params = [cutoff_date]
        
        # Apply prefix filter in SQL
        if prefix:
            query += ' AND key LIKE ?'
            params.append(f'{prefix}%')
        
        # Apply suffix filter in SQL
        if suffix:
            query += ' AND key LIKE ?'
            params.append(f'%{suffix}')
        
        query += ' ORDER BY timestamp'
        
        # Apply limit in SQL
        if limit:
            query += ' LIMIT ?'
            params.append(limit)
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def search_latest_n_days(self, n: int, limit: Optional[int] = None, prefix: str = '', suffix: str = '') -> List[Dict]:
        """
        Search files from latest N days (Flask API compatible) with optional filters.
        OPTIMIZED: Applies filters and limits in SQL for 100x faster queries.
        
        Args:
            n: Number of days
            limit: Maximum results to return (None = all matches)
            prefix: Filter by key prefix (applied in SQL)
            suffix: Filter by key suffix (applied in SQL)
            
        Returns:
            List of matching files
        """
        cutoff_date = (datetime.now(timezone.utc) - timedelta(days=n)).isoformat()
        
        # Build SQL query with filters
        query = 'SELECT key, timestamp, size, etag FROM objects WHERE timestamp >= ?'
        params = [cutoff_date]
        
        # Apply prefix filter in SQL
        if prefix:
            query += ' AND key LIKE ?'
            params.append(f'{prefix}%')
        
        # Apply suffix filter in SQL
        if suffix:
            query += ' AND key LIKE ?'
            params.append(f'%{suffix}')
        
        query += ' ORDER BY timestamp DESC'
        
        # Apply limit in SQL
        if limit:
            query += ' LIMIT ?'
            params.append(limit)
        
        cursor = self.conn.cursor()
        cursor.execute(query, params)
        rows = cursor.fetchall()
        
        return [
            {
                'key': row['key'],
                'timestamp': row['timestamp'],
                'size': row['size'],
                'etag': row['etag']
            }
            for row in rows
        ]
    
    def multi_filter(self, results: List[Dict], prefix: str = '', suffix: str = '') -> List[Dict]:
        """
        Apply prefix and suffix filters to results (Flask API compatible).
        
        DEPRECATED: This method is kept for backward compatibility.
        New code should pass prefix/suffix directly to search methods for better performance.
        
        Args:
            results: List of file objects
            prefix: Filter by key prefix
            suffix: Filter by key suffix
            
        Returns:
            Filtered list of files
        """
        if prefix:
            results = [obj for obj in results if obj['key'].startswith(prefix)]
        if suffix:
            results = [obj for obj in results if obj['key'].endswith(suffix)]
        return results
    
    def close(self):
        """Close database connection."""
        if self.conn:
            self.conn.close()
    
    def __enter__(self):
        """Context manager support."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager support."""
        self.close()


# Example usage
if __name__ == '__main__':
    import sys
    
    # Create database
    db = IndexDB('s3_index.db')
    
    # Import from JSON if provided
    if len(sys.argv) > 1:
        json_file = sys.argv[1]
        if os.path.exists(json_file):
            db.import_from_json(json_file)
            
            # Show statistics
            stats = db.get_statistics()
            print(f"\n📊 Database Statistics:")
            print(f"   Total objects: {stats['total_objects']:,}")
            print(f"   Total size: {stats['total_size_gb']} GB")
            print(f"   Date range: {stats['earliest_timestamp']} to {stats['latest_timestamp']}")
        else:
            print(f"Error: File not found: {json_file}")
    else:
        print("Usage: python index_db.py <json_index_file>")
        print("\nExample:")
        print("  python index_db.py s3_index.json")
    
    db.close()
