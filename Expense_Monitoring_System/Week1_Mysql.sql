-- Create Database
CREATE DATABASE personal_expenses;
USE personal_expenses;

-- Create Users Table
CREATE TABLE users (
    user_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);

-- Create Categories Table
CREATE TABLE categories (
    category_id INT AUTO_INCREMENT PRIMARY KEY,
    category_name VARCHAR(50) NOT NULL
);

-- Create Expenses Table
CREATE TABLE expenses (
    expense_id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    category_id INT NOT NULL,
    expense_date DATE NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    description TEXT,
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (category_id) REFERENCES categories(category_id)
);
-----------------------------------------------------
-- Insert Users
INSERT INTO users (name, email) VALUES
('Amit Sharma', 'amit.sharma@example.com'),
('Priya Nair', 'priya.nair@example.com'),
('Rohit Mehta', 'rohit.mehta@example.com'),
('Sneha Kulkarni', 'sneha.kulkarni@example.com'),
('Vikram Desai', 'vikram.desai@example.com'),
('Anita Rao', 'anita.rao@example.com');

-- Insert Categories
INSERT INTO categories (category_name) VALUES
('Food'),
('Transport'),
('Shopping'),
('Entertainment'),
('Utilities'),
('Healthcare');

-- Insert Expenses
INSERT INTO expenses (user_id, category_id, expense_date, amount, description) VALUES
(1, 1, '2025-01-05', 245.75, 'Groceries from D-Mart'),
(1, 2, '2025-01-15', 90.00, 'Auto fare to office'),
(2, 3, '2025-01-22', 1250.00, 'Winter jacket purchase'),
(3, 4, '2025-02-10', 350.00, 'Concert tickets'),
(4, 5, '2025-02-18', 1450.00, 'Electricity bill'),
(5, 6, '2025-03-05', 800.00, 'Dental check-up'),
(6, 1, '2025-03-15', 320.00, 'Dinner at Barbeque Nation'),
(2, 2, '2025-03-20', 60.00, 'Metro card recharge'),
(3, 3, '2025-03-25', 2200.00, 'Smartphone purchase'),
(4, 4, '2025-04-01', 500.00, 'Family movie outing');

-------------------------------

---------------------- CRUD OPERATIONS
-- View all users
SELECT * FROM users;

-- View all categories
SELECT * FROM categories;

-- View all expenses with user and category names
SELECT e.expense_id, u.name AS user_name, c.category_name, e.expense_date, e.amount, e.description
FROM expenses e
JOIN users u ON e.user_id = u.user_id
JOIN categories c ON e.category_id = c.category_id;

-- Update an expense (Example: Change expense description and amount for expense_id = 3)
UPDATE expenses
SET amount = 1300.00,
    description = 'Winter jacket + accessories purchase'
WHERE expense_id = 3;

-- Update a user's email (Example: User ID = 5)
UPDATE users
SET email = 'vikramd.new@example.com'
WHERE user_id = 5;

-- Delete a specific expense 
DELETE FROM expenses
WHERE expense_id = 8;

----------- stored procedure
DELIMITER //
CREATE PROCEDURE GetMonthlyCategoryTotals(IN in_user_id INT, IN in_month INT, IN in_year INT)
BEGIN
    SELECT c.category_name, SUM(e.amount) AS total_amount
    FROM expenses e
    JOIN categories c ON e.category_id = c.category_id
    WHERE e.user_id = in_user_id
      AND MONTH(e.expense_date) = in_month
      AND YEAR(e.expense_date) = in_year
    GROUP BY c.category_name;
END //
DELIMITER ;

-- Example usage:
CALL GetMonthlyCategoryTotals(1, 1, 2025);
