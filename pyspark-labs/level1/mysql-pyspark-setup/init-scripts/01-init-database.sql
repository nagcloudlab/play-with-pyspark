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
