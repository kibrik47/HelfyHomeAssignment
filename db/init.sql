CREATE DATABASE IF NOT EXISTS appdb;
USE appdb;

CREATE TABLE IF NOT EXISTS users (
  id INT AUTO_INCREMENT PRIMARY KEY,
  username VARCHAR(100) UNIQUE NOT NULL,
  email VARCHAR(200) UNIQUE,
  password VARCHAR(200) NOT NULL,
  token VARCHAR(255),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- default user (username: admin, password: password)
INSERT INTO users (username, email, password) VALUES
('admin', 'admin@example.com', 'password')
ON DUPLICATE KEY UPDATE username=username;
