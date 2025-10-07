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
