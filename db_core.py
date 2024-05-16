import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    conn = None
    try:
       conn = sqlite3.connect(db_file)
       return conn
    except Error as e:
       print(e)

    return conn

def execute_sql(conn, sql):
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_project(conn, project):
   sql = '''INSERT INTO projects(nazwa, start_date, end_date)
             VALUES(?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, project)
   conn.commit()
   return cur.lastrowid

def add_task(conn, task):
    sql = '''INSERT INTO tasks(projekt_id, nazwa, opis, status, start_date, end_date)
                VALUES(?,?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, task)
    conn.commit()
    return cur.lastrowid

def select_all(conn, table):
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Deleted")

def delete_all(conn, table):
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Deleted")


if __name__ == "__main__":

   create_projects_sql = """
   -- projects table
   CREATE TABLE IF NOT EXISTS projects (
      id integer PRIMARY KEY,
      nazwa text NOT NULL,
      start_date text,
      end_date text
   );
   """

   create_tasks_sql = """
   -- zadanie table
   CREATE TABLE IF NOT EXISTS tasks (
      id integer PRIMARY KEY,
      projekt_id integer NOT NULL,
      nazwa VARCHAR(250) NOT NULL,
      opis TEXT,
      status VARCHAR(15) NOT NULL,
      start_date text NOT NULL,
      end_date text NOT NULL,
      FOREIGN KEY (projekt_id) REFERENCES projects (id)
   );
   """

   db_file = "database.db"

   conn = create_connection(db_file)
   if conn is not None:
       execute_sql(conn, create_projects_sql)
       execute_sql(conn, create_tasks_sql)
       conn.close()

   conn = create_connection(db_file)
   project = ("Zadanie z kursu Python", "2024-05-16 18:45:00", "2024-05-16 19:45:00")
   pr_id = add_project(conn, project)
   task = (
      pr_id,
      "Praca z bazą danych SQLLite",
       "Przejdź przez wszystkie operacje na bazie",
       "started",
       "2024-05-16 18:45:00",
       "2024-05-16 19:15:00"
   )
   task_id = add_task(conn, task)
   print(select_all(conn, "projects"))
   print(select_where(conn, "tasks", projekt_id=1))
   update(conn, "tasks", 1, status="ended")
   task_id = add_task(conn, task)
   delete_where(conn, "tasks", id=2)
   print(select_all(conn, "tasks"))
   delete_all(conn, "tasks")
   conn.close()
  