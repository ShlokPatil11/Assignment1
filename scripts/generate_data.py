import csv
import random
import os
from faker import Faker
from datetime import datetime, timedelta

def generate_employee_data(num_records=1050, output_path='../data/employees_raw.csv'):
    fake = Faker()
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Pre-generate some employee IDs to allow for duplicates
    base_employee_ids = [fake.unique.random_int(min=10000, max=99999) for _ in range(int(num_records * 0.9))]
    
    # Data generation
    records = []
    
    for i in range(num_records):
        # ~10% duplicate employee_ids
        if random.random() < 0.10 and i > 0:
            emp_id = random.choice(records)['employee_id']
        else:
            emp_id = random.choice(base_employee_ids)
            
        first_name = fake.first_name()
        last_name = fake.last_name()
        
        # Mixed case for names (e.g. jOhN, SmiTh) - apply to some ~10%
        if random.random() < 0.1:
            first_name = ''.join(random.choice([c.upper(), c.lower()]) for c in first_name)
            last_name = ''.join(random.choice([c.upper(), c.lower()]) for c in last_name)
            
        # Email generation
        email = f"{first_name.lower()}.{last_name.lower()}@{fake.domain_name()}"
        # ~8% invalid emails (missing @domain)
        if random.random() < 0.08:
            email = email.split('@')[0]
            
        # Hire date
        hire_date = fake.date_between(start_date='-10y', end_date='today')
        # ~5% future hire_dates
        if random.random() < 0.05:
            hire_date = (datetime.now() + timedelta(days=random.randint(1, 365))).date()
            
        job_title = fake.job()
        
        department = random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance', 'IT', 'Operations'])
        # Mixed case department
        if random.random() < 0.1:
            department = ''.join(random.choice([c.upper(), c.lower()]) for c in department)
            
        # Salary sometimes has "$" and commas
        salary_int = random.randint(40000, 150000)
        if random.random() < 0.3:
            salary = f"${salary_int:,}"
        else:
            salary = str(salary_int)
            
        # Manager ID (null ~5% of time)
        manager_id = random.choice(base_employee_ids) if random.random() > 0.05 else ""
        
        # Address (null ~5% of time)
        address = fake.street_address().replace('\n', ', ') if random.random() > 0.05 else ""
        
        city = fake.city()
        state = fake.state_abbr()
        
        # Zip code (null ~5% of time)
        zip_code = fake.zipcode() if random.random() > 0.05 else ""
        
        birth_date = fake.date_between(start_date='-60y', end_date='-22y')
        status = random.choice(['Active', 'Inactive', 'On Leave'])
        
        records.append({
            'employee_id': emp_id,
            'first_name': first_name,
            'last_name': last_name,
            'email': email,
            'hire_date': hire_date,
            'job_title': job_title,
            'department': department,
            'salary': salary,
            'manager_id': manager_id,
            'address': address,
            'city': city,
            'state': state,
            'zip_code': zip_code,
            'birth_date': birth_date,
            'status': status
        })

    # Write to CSV
    fieldnames = ['employee_id', 'first_name', 'last_name', 'email', 'hire_date', 'job_title', 
                  'department', 'salary', 'manager_id', 'address', 'city', 'state', 'zip_code', 
                  'birth_date', 'status']
    
    # Path is relative to the script location
    script_dir = os.path.dirname(os.path.abspath(__file__))
    absolute_output_path = os.path.join(script_dir, output_path)
                  
    with open(absolute_output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)
        
    print(f"Successfully generated {num_records} records at {absolute_output_path}")

if __name__ == "__main__":
    generate_employee_data()
