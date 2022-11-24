#!/usr/bin/python3
import sqlite3
from os import path
import pandas as pd

DATABASE_PATH = path.join("/" + "db.sqlite3")
EXCLUDED_LIST = ["auth_user"]

class SqliteHelper():
    def __init__(self):
        self.cursor = None
        self.conn = None

    def connect(self):
        try:
            self.conn = sqlite3.connect(f"{DATABASE_PATH}")
            self.cursor = self.conn.cursor()
            print("Opening the connection")
        except Exception as e:
            print(e)

    def close(self):
        print("Closing the connection")
        self.conn.close()

    def _obtainTables(self) -> list:
        try:
            sql_command = """
            SELECT name FROM sqlite_master  
            WHERE type='table';"""
            self.cursor.execute(sql_command)
            self.all_tables: list = self.cursor.fetchall()
            
            return self.all_tables
        except AttributeError as e:
            print(f"AttributeError in obtainTables command: {e}")
        except Exception as e:
            print(f"Unable to execute the obtainTables command: {e}")
        
        return False


    def produceCSV(self):
        self._obtainTables()
        for table in self.all_tables:
            # Convert tuple index to string
            table_name: str = table[0]
            if table_name in EXCLUDED_LIST:
                continue
            print(table_name)
            query_string : str = f"SELECT * FROM {table_name}"
            db_df = pd.read_sql_query(query_string, self.conn)
            db_df.to_csv(f'{table_name}.csv', index=False)

        self.conn.close()
        

if (__name__ == "__main__"):
    print("Starting the script")
    instance = SqliteHelper()
    instance.connect()
    instance.produceCSV()
    instance.close()
