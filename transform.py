import os
import psycopg2
from datetime import datetime

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


def transform_and_load_aggregated_data(cursor):
    """Агрегирует данные из raw_users_by_posts и загружает в top_users_by_posts."""
    transform_query = """
          INSERT INTO top_users_by_posts (user_id, posts_count, calculated_at)
          SELECT
              user_id,
              COUNT(post_id) AS posts_count,
              NOW() AS calculated_at
          FROM
              raw_users_by_posts
          GROUP BY
              user_id
          ON CONFLICT (user_id) DO UPDATE SET
              posts_count = EXCLUDED.posts_count,
              calculated_at = NOW();
          """
    try:
        cursor.execute(transform_query)
        print("Витрина 'top_users_by_posts' обновлена.")
    except Exception as e:
        print(f"Ошибка при обновлении витрины 'top_users_by_posts': {e}")
        raise


if __name__ == "__main__":
    conn = None
    try:
        conn = get_db_connection()
        if conn:
            conn.autocommit = False
            with conn.cursor() as cursor:
                transform_and_load_aggregated_data(cursor)
            conn.commit()
            print("Transform/Load-транзакция завершена успешно.")
    except Exception as e:
        if conn:
            conn.rollback()
            print("Transform/Load-транзакция отменена из-за ошибки.")
        print(f"Произошла общая ошибка в transform.py: {e}")
    finally:
        if conn:
            conn.close()
            print("Соединение с базой данных закрыто.")