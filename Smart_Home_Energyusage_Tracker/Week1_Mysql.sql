-- Create Database
CREATE DATABASE SmartHomeEnergy;
USE SmartHomeEnergy;

-- Rooms Table
CREATE TABLE rooms (
    room_id INT AUTO_INCREMENT PRIMARY KEY,
    room_name VARCHAR(50) NOT NULL
);

-- Devices Table
CREATE TABLE devices (
    device_id INT AUTO_INCREMENT PRIMARY KEY,
    device_name VARCHAR(50) NOT NULL,
    room_id INT,
    status ENUM('ON', 'OFF') DEFAULT 'OFF',
    FOREIGN KEY (room_id) REFERENCES rooms(room_id)
);

-- Energy Logs Table
CREATE TABLE energy_logs (
    log_id INT AUTO_INCREMENT PRIMARY KEY,
    device_id INT,
    log_timestamp DATETIME,
    energy_kwh DECIMAL(6,2),
    FOREIGN KEY (device_id) REFERENCES devices(device_id)
);

---------------------------------

-- Insert into rooms
INSERT INTO rooms (room_name) VALUES
('Living Room'),
('Kitchen'),
('Bedroom');

-- Insert into devices
INSERT INTO devices (device_name, room_id, status) VALUES
('AC', 1, 'ON'),
('TV', 1, 'OFF'),
('Fridge', 2, 'ON'),
('Oven', 2, 'OFF'),
('Heater', 3, 'ON'),
('Light', 3, 'OFF');

-- Insert into energy_logs
INSERT INTO energy_logs (device_id, log_timestamp, energy_kwh) VALUES
(1, '2025-08-01 08:00:00', 2.5),
(2, '2025-08-01 09:00:00', 0.3),
(3, '2025-08-01 10:00:00', 1.2),
(4, '2025-08-01 13:00:00', 2.0),
(5, '2025-08-01 11:00:00', 1.8),
(6, '2025-08-01 17:00:00', 0.1);

------------------ CRUD OPERATIONS

----- Inserting
INSERT INTO devices (device_name, room_id, status)
VALUES ('Fan', 3, 'ON');

-------- Read
SELECT * FROM devices;
SELECT * FROM energy_logs WHERE device_id = 1;

------- Update
UPDATE devices SET status = 'OFF' WHERE device_id = 1;

------------ Delete
DELETE FROM devices WHERE device_id = 7; 

-------------- STORED PROCEDURE
DELIMITER //
CREATE PROCEDURE GetRoomEnergyUsage(IN usage_date DATE)
BEGIN
    SELECT r.room_name,
           DATE(e.log_timestamp) AS usage_day,
           SUM(e.energy_kwh) AS total_energy_kwh
    FROM energy_logs e
    JOIN devices d ON e.device_id = d.device_id
    JOIN rooms r ON d.room_id = r.room_id
    WHERE DATE(e.log_timestamp) = usage_date
    GROUP BY r.room_name, usage_day;
END //
DELIMITER ;

-- Call example:
CALL GetRoomEnergyUsage('2025-08-01');


