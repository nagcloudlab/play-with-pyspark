#!/usr/bin/env python3
"""
Test script to verify MySQL connection for PySpark examples
"""
import mysql.connector
import sys
from mysql.connector import Error

def test_mysql_connection():
    """Test MySQL connection and verify sample data"""
    try:
        print("🔌 Testing MySQL connection...")
        
        # Connection parameters
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            database='todosdb',
            user='root',
            password='root1234'
        )
        
        if connection.is_connected():
            cursor = connection.cursor()
            
            # Test queries
            print("✅ Successfully connected to MySQL")
            
            # Check database and tables
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"📋 Tables found: {[table[0] for table in tables]}")
            
            # Check sample data
            cursor.execute("SELECT COUNT(*) FROM todos")
            todo_count = cursor.fetchone()[0]
            print(f"📝 Todos count: {todo_count}")
            
            cursor.execute("SELECT COUNT(*) FROM users")
            user_count = cursor.fetchone()[0]
            print(f"👥 Users count: {user_count}")
            
            # Sample query for PySpark testing
            cursor.execute("""
                SELECT u.username, COUNT(*) as todo_count, 
                       SUM(CASE WHEN t.completed = 1 THEN 1 ELSE 0 END) as completed
                FROM users u 
                LEFT JOIN todos t ON u.id = t.user_id 
                GROUP BY u.id, u.username
            """)
            
            results = cursor.fetchall()
            print("\n👤 User productivity summary:")
            for username, total, completed in results:
                completion_rate = (completed/total*100) if total > 0 else 0
                print(f"   {username}: {completed}/{total} tasks ({completion_rate:.1f}%)")
            
            print("\n🎉 MySQL setup is ready for PySpark examples!")
            return True
            
    except Error as e:
        print(f"❌ MySQL connection failed: {e}")
        return False
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    success = test_mysql_connection()
    sys.exit(0 if success else 1)
