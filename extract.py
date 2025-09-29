
import os
import requests
import psycopg2
from datetime import datetime


API_URL = os.getenv('API_URL', 'https://jsonplaceholder.typicode.com/posts')
DB_HOST = os.getenv('DB_HOST', 'db')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')


def get_db_connection():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("Соединение с базой данных установлено успешно.")
        return conn
    except Exception as e:
        print(f"Ошибка при подключении к базе данных: {e}")
        raise


def extract_raw_data(cursor):
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        posts = response.json()
        print(f"Получено {len(posts)} постов из API.")

        insert_query = """
           INSERT INTO raw_users_by_posts (post_id, user_id, title, body, extracted_at)
           VALUES (%s, %s, %s, %s, %s)
           ON CONFLICT (post_id) DO UPDATE SET
               user_id = EXCLUDED.user_id,
               title = EXCLUDED.title,
               body = EXCLUDED.body,
               extracted_at = NOW();
           """
        data_to_insert = []
        for post in posts:
            data_to_insert.append((
                post.get('id'),
                post.get('userId'),
                post.get('title'),
                post.get('body'),
                datetime.now()
            ))

        if data_to_insert:
            cursor.executemany(insert_query, data_to_insert)
            print(f"Вставлено/обновлено {len(data_to_insert)} сырых записей в 'raw_users_by_posts'.")
        else:
            print("Нет данных для вставки в 'raw_users_by_posts'.")

    except requests.exceptions.RequestException as e:
        print(f"Ошибка HTTP-запроса: {e}")
        raise
    except ValueError as e:
        print(f"Ошибка парсинга JSON: {e}")
        raise
    except Exception as e:
        print(f"Ошибка при извлечении сырых данных: {e}")
        raise

    if __name__ == "__main__":
        conn = None
        try:
            conn = get_db_connection()
            if conn:
                conn.autocommit = False
                with conn.cursor() as cursor:
                    extract_raw_data(cursor)  # Только извлечение сырых данных
                conn.commit()
                print("Extract-транзакция завершена успешно.")
        except Exception as e:
            if conn:
                conn.rollback()
                print("Extract-транзакция отменена из-за ошибки.")
            print(f"Произошла общая ошибка в extract.py: {e}")
        finally:
            if conn:
                conn.close()
                print("Соединение с базой данных закрыто.")