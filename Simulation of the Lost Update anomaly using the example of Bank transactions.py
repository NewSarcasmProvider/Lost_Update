# Импортируем библиотеки
import psycopg2                  # Работа с PostgreSQL
import threading                 # Для запуска параллельных потоков (моделирование гонки данных)
import time                      # Для задержек и случайностей (не используется, но может пригодиться)
from datetime import datetime    # Для логирования времени
from dotenv import load_dotenv   # Загрузка переменных окружения из .env
import os

# Загружаем переменные окружения
load_dotenv()

# Параметры подключения к PostgreSQL (пароль берём из .env)
DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'database': 'bank_lab',
    'user': 'postgres',
    'password': os.getenv('PGPASSWORD')
}

# Создание таблиц базы данных
def setup_db():
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            # Таблица пользователей
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    id SERIAL PRIMARY KEY,
                    owner TEXT UNIQUE NOT NULL,
                    balance NUMERIC CHECK (balance >= 0)
                );
            """)
            # Таблица транзакций между аккаунтами
            cur.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    from_account INT,
                    to_account INT,
                    amount NUMERIC,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
    print("Database setup complete.")

# Заполнение базы данных начальными аккаунтами с откатом при ошибке (через SAVEPOINT)
def seed_accounts_transactional(accounts):
    with psycopg2.connect(**DB_PARAMS) as conn:
        conn.set_session(autocommit=False)
        try:
            with conn.cursor() as cur:
                cur.execute("BEGIN;")
                for name, balance in accounts:
                    cur.execute("SAVEPOINT sp_before_insert;")  # Точка возврата
                    try:
                        # Проверяем, есть ли уже такой пользователь
                        cur.execute("SELECT COUNT(*) FROM accounts WHERE owner = %s;", (name,))
                        if cur.fetchone()[0] > 0:
                            raise Exception(f"Имя пользователя '{name}' уже занято.")
                        # Пытаемся вставить аккаунт
                        cur.execute("INSERT INTO accounts (owner, balance) VALUES (%s, %s);", (name, balance))
                        print(f"Добавлен аккаунт: {name}")
                    except Exception as e:
                        # Если ошибка — откат к SAVEPOINT
                        cur.execute("ROLLBACK TO SAVEPOINT sp_before_insert;")
                        print(f"Пропущен аккаунт {name}: {e}")

                conn.commit()
                print("Аккаунты добавлены частично (в случае ошибок).")
        except Exception as e:
            conn.rollback()
            print(f"Общая ошибка: {e}")

# Транзакция перевода средств между двумя счетами с учётом уровня изоляции
def transfer(from_id, to_id, amount, isolation_level="READ COMMITTED"):
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.set_session(isolation_level=isolation_level, autocommit=False)
        with conn.cursor() as cur:
            cur.execute("BEGIN;")

            # Искусственная задержка для создания race condition
            time.sleep(5)

            cur.execute("SELECT balance FROM accounts WHERE id = %s FOR UPDATE;", (from_id,))
            row = cur.fetchone()
            if not row:
                raise Exception(f"Счёт {from_id} не найден.")
            from_balance = float(row[0])

            # Проверка достаточности средств
            if from_balance < amount:
                raise Exception(f"Недостаточно средств на счёте {from_id}")

            # Обновляем балансы: списание у отправителя, зачисление у получателя
            cur.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s;", (amount, from_id))
            cur.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s;", (amount, to_id))

            # Логируем перевод в таблицу транзакций
            cur.execute("INSERT INTO transactions (from_account, to_account, amount) VALUES (%s, %s, %s);",
                        (from_id, to_id, amount))
            conn.commit()
            print(f"[{datetime.now()}] Перевод {amount} от #{from_id} к #{to_id} завершён.")
    except Exception as e:
        conn.rollback()
        print(f"[{datetime.now()}] Ошибка транзакции: {e}")
    finally:
        conn.close()

# Очистка базы данных: удаляет все данные из таблиц и сбрасывает счётчики ID
def clear_db():
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            # Очищаем таблицу транзакций, если она есть
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'transactions'
                );
            """)
            if cur.fetchone()[0]:
                cur.execute("DELETE FROM transactions;")
                cur.execute("ALTER SEQUENCE transactions_id_seq RESTART WITH 1;")

            # Очищаем таблицу аккаунтов, если она есть
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'accounts'
                );
            """)
            if cur.fetchone()[0]:
                cur.execute("DELETE FROM accounts;")
                cur.execute("ALTER SEQUENCE accounts_id_seq RESTART WITH 1;")

            conn.commit()
    print("База данных очищена и последовательности сброшены (если таблицы существовали).")

from tabulate import tabulate  # Для красивого вывода таблиц в консоли

# Печать содержимого таблицы
def print_table(table_name):
    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name};")
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            print(f"\nСодержимое таблицы '{table_name}':")
            print(tabulate(rows, headers=colnames, tablefmt="fancy_grid"))

# МОДЕЛИРОВАНИЕ АНОМАЛИИ: Потерянное обновление
# Два потока параллельно списывают средства с одного счёта — может привести к потере данных
def simulate_lost_update(isolation_level="READ COMMITTED"):
    print(f"\n Потерянное обновление (Isolation: {isolation_level})")

    # Два перевода одновременно списывают средства со счёта №1
    thread1 = threading.Thread(target=transfer, args=(1, 2, 100, isolation_level))
    thread2 = threading.Thread(target=transfer, args=(1, 3, 200, isolation_level))

    thread1.start()
    thread2.start()

    thread1.join()
    thread2.join()

    print_table("transactions")
    print_table("accounts")

# Основной блок программы
if __name__ == "__main__":
    clear_db()       # Очищаем таблицы и сбрасываем счётчики
    setup_db()       # Создаём таблицы заново

    # Исходные аккаунты: Alice — дублируется, David — с отрицательным балансом
    accounts = [
        ("Alice", 1000),
        ("Bob", 1500),
        ("Charlie", 2000),
        ("Alice", 500),       # Ошибка: дубликат owner
        ("David", -100),      # Ошибка: баланс < 0 (нарушение CHECK)
        ("Eve", 3000)
    ]

    seed_accounts_transactional(accounts)  # Добавляем аккаунты с обработкой ошибок

    print_table("accounts")  # Печатаем состояние таблицы accounts

    # Моделируем потерянное обновление при разных уровнях изоляции
    simulate_lost_update("READ COMMITTED")   # возможна аномалия
    simulate_lost_update("REPEATABLE READ")  # может предотвратить
