"""**Домашнее задание к лекции «Работа с PostgreSQL из Python»**

Создайте программу для управления клиентами на python.

Требуется хранить персональную информацию о клиентах:

-имя

-фамилия

-email

-телефон

Сложность в том, что телефон у клиента может быть не один, а два, три и даже больше. А может и вообще не быть телефона (например, он не захотел его оставлять).

Вам необходимо разработать структуру БД для хранения информации и несколько функций на python для управления данными:

Функция, создающая структуру БД (таблицы)
Функция, позволяющая добавить нового клиента
Функция, позволяющая добавить телефон для существующего клиента
Функция, позволяющая изменить данные о клиенте
Функция, позволяющая удалить телефон для существующего клиента
Функция, позволяющая удалить существующего клиента
Функция, позволяющая найти клиента по его данным (имени, фамилии, email-у или телефону)
Функции выше являются обязательными, но это не значит что должны быть только они. При необходимости можете создавать дополнительные функции и классы.

Также предоставьте код, демонстрирующий работу всех написанных функций.

Результатом работы будет .py файл."""

from pprint import pprint
import psycopg2

# создаем базу данных
def create_db(conn):
    conn = psycopg2.connect(dbname="postgres", user="postgres", password="***", host="127.0.0.1")
        # создание БД
    conn.autocommit = True
    sql = "CREATE DATABASE test_db1"
        # выполняем запрос
    with conn.cursor() as cur:
        cur.execute(sql)
        print("База данных test_db1 успешно создана!!!")
        conn.close()
# создаем таблицу clients и phonenumbers
def create_tables(conn):

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                first_name TEXT NOT NULL,
                last_name TEXT NOT NULL,
                email TEXT UNIQUE
                );
            """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS phonenumbers (
                id SERIAL PRIMARY KEY,
                client_id INTEGER NOT NULL REFERENCES clients(id) ON DELETE CASCADE,
                phone TEXT UNIQUE
                );
            """)
        conn.commit()
        return

# добавляем нового клиента в таблицу clients и телефон в phonenumbers
def insert_client(conn, first_name, last_name, email, phone=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO clients(first_name, last_name, email)
            VALUES (%s, %s, %s)
            """, (first_name, last_name, email))
        cur.execute("""
            SELECT id from clients
            ORDER BY id DESC
            LIMIT 1
            """)
        client_id = cur.fetchone()[0]
        if phone:
            cur.execute(
                "INSERT INTO phonenumbers (client_id, phone) VALUES (%s, %s)",
                (client_id, phone)
            )
        conn.commit()
        return client_id


# добавляем телефон для существующего клиента.
def insert_tel(conn, client_id, phone):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO phonenumbers(client_id, phone)
            VALUES (%s, %s)
            """, (client_id, phone))
        conn.commit()
        return client_id

# обновляем данные клиента
def update_client(conn, client_id, first_name=None, last_name=None, email=None):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT * from clients
            WHERE id = %s
            """, (client_id, ))
        info = cur.fetchone()
        if first_name is None:
            first_name = info[1]
        if last_name is None:
            last_name = info[2]
        if email is None:
            email = info[3]
        cur.execute("""
            UPDATE clients
            SET first_name = %s, last_name = %s, email =%s
            where id = %s
            """, (first_name, last_name, email, client_id))
        conn.commit()
        return client_id

# удаляем телефон по его номеру.
def delete_phone(conn, phone):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phonenumbers
            WHERE phone = %s
            """, (phone, ))
        conn.commit()
        return phone

# Функция, позволяющая удалить существующего клиента
def delete_client(conn, client_id):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM phonenumbers
            WHERE client_id = %s
            """, (client_id, ))
        cur.execute("""
            DELETE FROM clients
            WHERE id = %s
           """, (client_id,))
        conn.commit()
        return client_id

# найти клиентов по любому сочетанию полей
def find_client(conn, first_name=None, last_name=None, email=None, phone=None):
    with conn.cursor() as cur:
        if first_name is None:
            first_name = '%'
        else:
            first_name = '%' + first_name + '%'
        if last_name is None:
            last_name = '%'
        else:
            last_name = '%' + last_name + '%'
        if email is None:
            email = '%'
        else:
            email = '%' + email + '%'
        if phone is None:
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                WHERE c.first_name LIKE %s AND c.last_name LIKE %s
                AND c.email LIKE %s
                """, (first_name, last_name, email))
        else:
            cur.execute("""
                SELECT c.id, c.first_name, c.last_name, c.email, p.phone FROM clients c
                LEFT JOIN phonenumbers p ON c.id = p.client_id
                WHERE c.first_name LIKE %s AND c.last_name LIKE %s
                AND c.email LIKE %s AND p.phone like %s
                """, (first_name, last_name, email, phone))
        return cur.fetchall()

# удаление таблиц
def delete_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
            DROP TABLE IF EXISTS phonenumbers;
            DROP TABLE IF EXISTS clients;
            """)
        conn.commit()
if __name__ == '__main__':

    conn = psycopg2.connect(dbname="postgres", user="postgres", password="***", host="127.0.0.1", options='-c client_encoding=UTF8')
            # создаем БД
    create_db(conn)
            # подключение к созданной БД
    conn = psycopg2.connect(dbname="test_db1", user="postgres", password="***", host="127.0.0.1", options='-c client_encoding=UTF8')
            # Удаление таблиц перед запуском
    delete_db(conn)
            # 1. Cоздание таблиц
    create_tables(conn)
    print("Таблицы созданы")
           
            # Примеры вставки
    print("Добавлен клиент id:", insert_client(conn, "Валерий", "Петров", "dassa@mail.ru", "89544778899"))
    print("Добавлен клиент id:", insert_client(conn, "Петр", "Юнусов", "k8sjebg1y@mail.ru", "89544774899"))
    print("Добавлен клиент id:", insert_client(conn, "Ктерина", "Евдокимовна", "19dn@outlook.com", "89544778199"))

            # Добавляем телефоны
    print("Телефон добавлен, id:", insert_tel(conn, 2, "12345678911"))
    print("Телефон добавлен, id:", insert_tel(conn, 1, "12345678912"))

            # Изменение клиента
    print("Изменены данные клиента id:", update_client(conn, 3, "Иван", None, "dsds@mail.ru"))

            # Удаление телефона
    print("Телефон удалён, id:", delete_phone(conn, "12345678912"))

            # Удаление клиента
    print("Клиент удалён, id:", delete_client(conn, 2))

            # Поиск
    pprint(find_client(conn, "Валерий"))
    pprint(find_client(conn, None, None, "19dn@outlook.com"))
    pprint(find_client(conn, "Валерий", "Петров", "dassa@mail.ru"))
    pprint(find_client(conn, "Иван", "Юнусов", "19dn@outlook.com", "89544774899"))
    pprint(find_client(conn, None, None, None, "89544778899"))
    conn.close()

