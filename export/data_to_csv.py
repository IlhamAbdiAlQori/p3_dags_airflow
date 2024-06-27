import pandas as pd
from sqlalchemy import create_engine
import os

# Koneksi ke database
DATABASE_URL = 'postgresql+psycopg2://postgres:123@172.27.206.206:5432/dataops'
engine = create_engine(DATABASE_URL)

# function untuk export dari table ke csv
def export_table_to_csv(table_name, output_folder):
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, engine)
    output_file = os.path.join(output_folder, f"{table_name}.csv")
    df.to_csv(output_file, index=False)
    print(f"Data from {table_name} has been exported to {output_file}")

# ambil semua table di db
query = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' and table_name not like 'datamart%%'
"""

table_names = pd.read_sql(query, engine)['table_name']

# export semua table ke folder dibawah
output_folder = r'C:\Users\Ilham\airflow\result'
for table_name in table_names:
    export_table_to_csv(table_name, output_folder)
