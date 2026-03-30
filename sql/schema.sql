-- Create table for cleaned employee data
CREATE TABLE IF NOT EXISTS employees_clean (
    employee_id INTEGER PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    email_domain VARCHAR(100),
    job_title VARCHAR(255),
    department VARCHAR(100),
    salary NUMERIC(12, 2),
    salary_band VARCHAR(50),
    manager_id INTEGER,
    address TEXT,
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    status VARCHAR(50),
    age INTEGER,
    tenure_years INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for frequently queried columns
CREATE INDEX IF NOT EXISTS idx_employees_department ON employees_clean(department);
CREATE INDEX IF NOT EXISTS idx_employees_salary_band ON employees_clean(salary_band);
CREATE INDEX IF NOT EXISTS idx_employees_status ON employees_clean(status);
