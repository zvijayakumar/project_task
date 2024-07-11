import pandas as pd
import sqlite3
from datetime import datetime
import os
from airflow.exceptions import AirflowException

class ETLProcess:
    def __init__(self, csv_file, db_file):
        self.csv_file = csv_file
        self.db_file = db_file
        self.df = None
    
    def ingest_data(self):
        if not os.path.exists(self.csv_file):
            raise AirflowException(f"The file {self.csv_file} does not exist.")
        
        self.df = pd.read_csv(self.csv_file)
        
        if self.df.empty:
            raise AirflowException(f"The file {self.csv_file} contains no data.")
        
        print("Printing the first 5 rows:")
        print(self.df.head(5))
    
    def clean_data(self):
        if self.df is None:
            raise AirflowException("Data not ingested yet. Call ingest_data() first.")
        
        self.df.dropna(inplace=True)
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'])
    
    def transform_data(self):
        if self.df is None:
            raise AirflowException("Data not ingested yet. Call ingest_data() first.")
        
        self.df['interaction_count_user'] = self.df.groupby('user_id')['interaction_id'].transform('count')
        self.df['interaction_count_user_product'] = self.df.groupby(['user_id', 'product_id'])['interaction_id'].transform('count')
    
    def load_data(self):
        if self.df is None:
            raise AirflowException("Data not ingested yet. Call ingest_data() first.")
        
        try:
            conn = sqlite3.connect(self.db_file)
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS interactions (
                    interaction_id INTEGER PRIMARY KEY,
                    user_id INTEGER,
                    product_id INTEGER,
                    action TEXT,
                    timestamp TIMESTAMP,
                    interaction_count_user INTEGER,
                    interaction_count_user_product INTEGER
                )
            ''')
            
            self.df.to_sql('interactions', conn, if_exists='replace', index=False)
            
            conn.commit()
        except sqlite3.Error as e:
            print(f"An error occurred: {e}")
            raise AirflowException("SQL Lite Error has occured.")
        finally:
            if conn:
                conn.close()
