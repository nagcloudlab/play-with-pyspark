#!/bin/bash

# ============================================================================
# MySQL Docker Setup Script for PySpark Examples
# ============================================================================
# This script sets up MySQL in Docker with sample data for PySpark JDBC testing

set -e  # Exit on any error

echo "🐳 Setting up MySQL Docker environment for PySpark..."

# Create project directory structure
echo "📁 Creating project structure..."
mkdir -p mysql-pyspark-setup/init-scripts
cd mysql-pyspark-setup

# Create docker-compose.yml
echo "📝 Creating docker-compose.yml..."
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  mysql:
    image: mysql:8.0
    container_name: pyspark_mysql
    environment:
      MYSQL_ROOT_PASSWORD: root1234
      MYSQL_DATABASE: todosdb
      MYSQL_USER: pyspark_user
      MYSQL_PASSWORD: pyspark123
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./init-scripts:/docker-entrypoint-initdb.d
    command: --default-authentication-plugin=mysql_native_password
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      timeout: 5s
      retries: 10

  phpmyadmin:
    image: phpmyadmin/phpmyadmin
    container_name: pyspark_phpmyadmin
    environment:
      PMA_HOST: mysql
      PMA_PORT: 3306
      PMA_USER: root
      PMA_PASSWORD: root1234
    ports:
      - "8080:80"
    depends_on:
      - mysql

volumes:
  mysql_data:
EOF

# Create initialization SQL script
echo "📝 Creating database initialization script..."
cat > init-scripts/01-init-database.sql << 'EOF'
-- Create database and tables for PySpark examples
CREATE DATABASE IF NOT EXISTS todosdb;
USE todosdb;

-- Users table
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE
);

-- Categories table  
CREATE TABLE categories (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    color VARCHAR(7),
    description TEXT
);

-- Todos table
CREATE TABLE todos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    completed BOOLEAN DEFAULT FALSE,
    priority ENUM('LOW', 'MEDIUM', 'HIGH') DEFAULT 'MEDIUM',
    user_id INT NOT NULL,
    category_id INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    due_date DATE,
    INDEX idx_user_id (user_id),
    INDEX idx_completed (completed),
    INDEX idx_created_at (created_at)
);

-- Insert sample data
INSERT INTO users (username, email, first_name, last_name, department) VALUES
('john_doe', 'john.doe@company.com', 'John', 'Doe', 'Engineering'),
('jane_smith', 'jane.smith@company.com', 'Jane', 'Smith', 'Data Science'),
('mike_wilson', 'mike.wilson@company.com', 'Mike', 'Wilson', 'DevOps'),
('sarah_johnson', 'sarah.johnson@company.com', 'Sarah', 'Johnson', 'Engineering'),
('david_brown', 'david.brown@company.com', 'David', 'Brown', 'Data Science');

INSERT INTO categories (name, color, description) VALUES
('Development', '#007bff', 'Software development tasks'),
('Testing', '#28a745', 'QA and testing activities'),
('Documentation', '#ffc107', 'Documentation and knowledge sharing'),
('Meeting', '#6f42c1', 'Meetings and collaborative sessions'),
('Research', '#fd7e14', 'Research and investigation tasks');

INSERT INTO todos (title, description, completed, priority, user_id, category_id, due_date) VALUES
('Learn PySpark JDBC', 'Master PySpark database connectivity', FALSE, 'HIGH', 1, 1, '2024-11-15'),
('Setup MySQL Docker', 'Configure MySQL in Docker for development', TRUE, 'MEDIUM', 2, 1, '2024-10-20'),
('Build ETL Pipeline', 'Create data pipeline with PySpark', FALSE, 'HIGH', 1, 1, '2024-11-30'),
('Write Unit Tests', 'Add test coverage for data transformations', FALSE, 'MEDIUM', 3, 2, '2024-11-25'),
('Deploy to Production', 'Deploy PySpark job to production cluster', FALSE, 'HIGH', 2, 1, '2024-12-01'),
('Code Review', 'Review PySpark ETL implementation', TRUE, 'MEDIUM', 4, 1, '2024-10-25'),
('Performance Optimization', 'Optimize PySpark job performance', FALSE, 'LOW', 5, 1, '2024-12-10'),
('Documentation Update', 'Update technical documentation', TRUE, 'LOW', 3, 3, '2024-10-28'),
('Database Migration', 'Migrate legacy data to new schema', FALSE, 'HIGH', 4, 1, '2024-11-20'),
('Monitoring Setup', 'Implement job monitoring and alerting', FALSE, 'MEDIUM', 5, 1, '2024-11-18');
EOF

# Create connection test script
echo "📝 Creating connection test script..."
cat > test-connection.py << 'EOF'
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
EOF

# Make test script executable
chmod +x test-connection.py

# Create README with instructions
echo "📝 Creating README with instructions..."
cat > README.md << 'EOF'
# MySQL Docker Setup for PySpark Examples

This setup provides a MySQL database with sample data for testing PySpark JDBC connectivity.

## Quick Start

1. **Start MySQL container:**
   ```bash
   docker-compose up -d
   ```

2. **Wait for MySQL to be ready (30-60 seconds):**
   ```bash
   docker-compose logs -f mysql
   ```
   Wait for: `ready for connections`

3. **Test connection:**
   ```bash
   python3 test-connection.py
   ```

## Connection Details

- **Host:** localhost
- **Port:** 3306
- **Database:** todosdb
- **Username:** root
- **Password:** root1234

## Web Interface

Access phpMyAdmin at: http://localhost:8080
- Username: root
- Password: root1234

## Sample Data

The database includes:
- 5 users across different departments
- 5 task categories
- 10+ todo items with various completion states
- Proper relationships and indexes for PySpark testing

## PySpark Connection Example

```python
db_config = {
    "url": "jdbc:mysql://localhost:3306/todosdb",
    "driver": "com.mysql.cj.jdbc.Driver", 
    "user": "root",
    "password": "root1234"
}

df = spark.read.format("jdbc").options(**db_config).option("dbtable", "todos").load()
```

## Useful Commands

```bash
# Stop containers
docker-compose down

# Remove all data (fresh start)
docker-compose down -v

# View logs
docker-compose logs mysql

# Connect via MySQL CLI
docker exec -it pyspark_mysql mysql -u root -p todosdb
```

## Troubleshooting

1. **Port 3306 already in use:** Stop local MySQL or change port in docker-compose.yml
2. **Connection refused:** Wait longer for MySQL startup or check logs
3. **Authentication issues:** Ensure you're using the correct credentials
EOF

# Final setup
echo "🚀 Starting MySQL container..."
docker-compose up -d

echo ""
echo "⏳ Waiting for MySQL to start (this may take 30-60 seconds)..."
echo "   You can monitor progress with: docker-compose logs -f mysql"
echo ""
echo "🎯 Once MySQL is ready, test the connection with:"
echo "   python3 test-connection.py"
echo ""
echo "🌐 Access phpMyAdmin at: http://localhost:8080"
echo "   Username: root"
echo "   Password: root1234"
echo ""
echo "📚 See README.md for complete usage instructions"

# Wait a bit and test if MySQL is responding
# sleep 10
# echo ""
# echo "🔍 Quick health check..."
# if docker-compose exec mysql mysqladmin ping -h localhost --silent; then
#     echo "✅ MySQL is responding!"
#     echo "🧪 Running connection test..."
#     python3 test-connection.py 2>/dev/null || echo "⚠️  Connection test failed - MySQL may still be starting up"
# else
#     echo "⏳ MySQL still starting up - run 'python3 test-connection.py' in a minute"
# fi