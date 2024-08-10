-- Create a new user with the username 'airflow' and password 'airflow'
CREATE USER airflow WITH PASSWORD 'airflow';
-- Create a new database named 'airflow'
CREATE DATABASE airflow OWNER airflow;
-- Grant all privileges on the 'airflow' database to the 'airflow' user
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
