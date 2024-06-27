import pandas as pd
from sqlalchemy import create_engine
import os

# jika beda direktori
source_folder = os.path.join(os.path.dirname(__file__), '..','source')

# koneksi ke db
DATABASE_URL = 'postgresql+psycopg2://postgres:123@172.27.206.206:5432/dataops'
engine = create_engine(DATABASE_URL)

# nama file dan table nya
csv_files = {
    os.path.join(source_folder, 'categories.csv'): 'categories',
    os.path.join(source_folder, 'customers.csv'): 'customers',
    os.path.join(source_folder, 'employee_territories.csv'): 'employee_territories',
    os.path.join(source_folder, 'employees.csv'): 'employees',
    os.path.join(source_folder, 'orders.csv'): 'orders',
    os.path.join(source_folder, 'order_details.csv'): 'order_details',
    os.path.join(source_folder, 'products.csv'): 'products',
    os.path.join(source_folder, 'regions.csv'): 'regions',
    os.path.join(source_folder, 'shippers.csv'): 'shippers',
    os.path.join(source_folder, 'suppliers.csv'): 'suppliers',
    os.path.join(source_folder, 'territories.csv'): 'territories'
}

# Function untuk import
def import_csv_to_postgres(file_path, table_name):
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Data from {file_path} has been imported to table {table_name}")

# Import semua file CSV
for file_path, table_name in csv_files.items():
    import_csv_to_postgres(file_path, table_name)
