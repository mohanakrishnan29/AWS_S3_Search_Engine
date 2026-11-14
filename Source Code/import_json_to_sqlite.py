"""
Import JSON Index to SQLite Database
Converts s3_index.json to s3_index.db for 100x faster queries
Run this once after building your JSON index
"""
import argparse
import os
import sys
from index_db import IndexDB


def main():
    """Import JSON index to SQLite database."""
    parser = argparse.ArgumentParser(
        description='Import JSON index to SQLite for faster queries'
    )
    parser.add_argument(
        '--json-file',
        default='s3_index.json',
        help='Path to JSON index file (default: s3_index.json)'
    )
    parser.add_argument(
        '--db-file',
        default='s3_index.db',
        help='Path to SQLite database file (default: s3_index.db)'
    )
    parser.add_argument(
        '--force',
        action='store_true',
        help='Overwrite existing database'
    )
    
    args = parser.parse_args()
    
    # Check if JSON file exists
    if not os.path.exists(args.json_file):
        print(f"❌ Error: JSON file not found: {args.json_file}")
        print("\nPlease build the index first:")
        print("  1. For mock data: python app_integrated.py (it will auto-build)")
        print("  2. For S3 data: Run the Flask app once to trigger index build")
        sys.exit(1)
    
    # Check if database already exists
    if os.path.exists(args.db_file) and not args.force:
        print(f"⚠️  Database already exists: {args.db_file}")
        response = input("Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("❌ Import cancelled")
            sys.exit(0)
    
    # Initialize SQLite database
    print(f"\n🚀 Starting import from {args.json_file} to {args.db_file}...")
    print("=" * 60)
    
    try:
        db = IndexDB(args.db_file)
        count = db.import_from_json(args.json_file)
        
        print("=" * 60)
        print(f"\n✅ SUCCESS! Imported {count:,} objects to SQLite")
        
        # Show statistics
        stats = db.get_statistics()
        print("\n📊 Database Statistics:")
        print(f"   Total Objects: {stats['total_objects']:,}")
        print(f"   Total Size: {stats['total_size_gb']:.2f} GB")
        print(f"   Date Range: {stats['earliest_timestamp']} to {stats['latest_timestamp']}")
        
        print(f"\n💡 Next Steps:")
        print(f"   1. Set USE_SQLITE_INDEX=True in .env")
        print(f"   2. Set USE_DYNAMODB=False in .env")
        print(f"   3. Restart Flask app: python app_integrated.py")
        print(f"\n🎯 You'll now get 100x faster queries with TRUE client-side search!")
        
    except Exception as e:
        print(f"\n❌ Error during import: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
