import pandas as pd
import psycopg2

db_params = {
    'dbname': 'postgres',
    'user': 'postgres',
    'password': '1111',
    'host': 'localhost',
    'port': '5432'
}


csv_file_path = r'C:\Users\SAMSUNG\Desktop\oo.csv'


table_name = ''

try:
    
    df = pd.read_csv(csv_file_path, encoding='utf-8-sig', delimiter=';')
    print("CSV файл успешно прочитан.")
    print(df.head())  # Вывод первых нескольких строк для проверки
except Exception as e:
    print(f"Ошибка при чтении CSV файла: {e}")
    exit()

try:
    
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    print("Соединение с базой данных PostgreSQL установлено.")
    
    
    for index, row in df.iterrows():
        try:
         
            insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s'] * len(df.columns))})"
            cursor.execute(insert_query, tuple(row))
            print(f"Вставлено: {tuple(row)}")
        except Exception as e:
            print(f"Ошибка при вставке данных в строке {index}: {e}")
    
    
    conn.commit()
    print("Данные успешно импортированы в таблицу PostgreSQL.")
    
except Exception as e:
    print(f"Ошибка при вставке данных в базу данных PostgreSQL: {e}")

finally:
   
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    print("Соединение с базой данных PostgreSQL закрыто.")