from pymongo import MongoClient
from pprint import pprint

def connect_mongodb(host='localhost', port=27017, db_name='bigdata'):
    """Kết nối đến MongoDB"""
    try:
        client = MongoClient(f'mongodb://{host}:{port}/')
        db = client[db_name]
        print(f"✅ Connected to MongoDB: {db_name}")
        return client, db
    except Exception as e:
        print(f"❌ Error connecting to MongoDB: {e}")
        return None, None

def list_all_databases(client):
    """Liệt kê tất cả databases"""
    print("\n📁 Available Databases:")
    for db_name in client.list_database_names():
        print(f"  - {db_name}")

def list_all_collections(db):
    """Liệt kê tất cả collections trong database"""
    print(f"\n📚 Collections in '{db.name}':")
    collections = db.list_collection_names()
    if not collections:
        print("  (empty)")
    for coll_name in collections:
        count = db[coll_name].count_documents({})
        print(f"  - {coll_name}: {count} documents")
    return collections

def read_collection_sample(db, collection_name, limit=5):
    """Đọc một số documents mẫu từ collection"""
    print(f"\n📄 Sample documents from '{collection_name}':")
    collection = db[collection_name]
    
    # Lấy một số documents đầu tiên
    documents = collection.find().limit(limit)
    
    for i, doc in enumerate(documents, 1):
        print(f"\n--- Document {i} ---")
        pprint(doc)

def get_collection_stats(db, collection_name):
    """Lấy thống kê về collection"""
    print(f"\n📊 Statistics for '{collection_name}':")
    stats = db.command("collstats", collection_name)
    print(f"  - Total documents: {stats.get('count', 0)}")
    print(f"  - Storage size: {stats.get('storageSize', 0)} bytes")
    print(f"  - Total indexes: {stats.get('nindexes', 0)}")
    print(f"  - Index size: {stats.get('totalIndexSize', 0)} bytes")

def main():
    # Kết nối đến MongoDB từ docker-compose
    client, db = connect_mongodb(host='localhost', port=27017, db_name='tft_db')
    
    if not client:
        return
    
    try:
        # Liệt kê databases
        list_all_databases(client)
        
        # Liệt kê collections
        collections = list_all_collections(db)
        
        # Đọc sample từ mỗi collection
        for coll_name in collections:
            get_collection_stats(db, coll_name)
            read_collection_sample(db, coll_name, limit=2)
        
    finally:
        client.close()
        print("\n✅ Connection closed")

if __name__ == "__main__":
    main()