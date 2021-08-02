import sqlite3
import csv
import io
from datetime import date


class Database:

    def __init__(self, db_name):
        try:
            self.connection = sqlite3.connect(db_name, isolation_level=None)
        except sqlite3.OperationalError:
            connect_db = sqlite3.connect(+str(db_name)+'.db')
            connect_db.close()
            self.connection = sqlite3.connect(db_name, isolation_level=None)

    def close(self):
        self.connection.close()

    def table_exist(self, table_name):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='%s'", [table_name])
            return cursor.fetchone()
        except sqlite3.ProgrammingError:
            self.create_table()

    def create_table(self):
        query = ("""
        CREATE TABLE if not exists banks (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                code TEXT,
                creation_date DATE NOT NULL
        );
        """)
        self.connection.execute(query)

    def insertInto(self, table_name, columns, data):
        self.table_exist(table_name)
        query = "INSERT INTO {table} ({columns}) VALUES ({parameters})".format(
            table=table_name,
            columns=', '.join(columns),
            parameters=', '.join(['?'] * len(columns))
        )

        self.connection.execute(query, data)

    def select_db(self, table_name, code):
        self.table_exist(table_name)
        query = ("""SELECT code, name FROM '%s' WHERE code=?""" % table_name)
        cursor = self.connection.cursor()
        cursor.execute(query, (code,))
        return cursor.fetchall()

    def select_banks_db(self, table_name):
        self.table_exist(table_name)
        query = ("""SELECT code, name FROM '%s' WHERE code <> 'n/a' ORDER BY ABS(code)""" % table_name)
        cursor = self.connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()

    def backup(self, destination):
        with io.open(destination, 'w') as f:
            for linha in self.connection.iterdump():
                f.write('%s\n' % linha)


def reader(csv_name, destination_db):
    db_backup = Database(db_name='./api/banks.db')
    db_backup.backup(destination=destination_db)

    with open(csv_name) as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')
        csv_reader.__next__()
        db_table = Database(db_name='./api/banks.db')
        db_table.table_exist('banks')

        for row in csv_reader:
            db = Database(db_name='./api/banks.db')
            columndata = ('name', 'code', 'creation_date')
            try:
                if row:
                    query = db.select_db('banks', str(row[2]))
                    if not query:
                        data = (str(row[1]), str(row[2]), str(date.today()))
                        db.insertInto('banks', columndata, data)
                        db.close()
                    else:
                        db.close()
                        continue
                else:
                    continue
            except IndexError:
                print(str(row))


def select_api(code):
    db = Database(db_name='./api/banks.db')
    query = db.select_db('banks', str(code))
    data = {
        'code': str(code),
        'name': (dict(query).get(str(code), 'Bank does not exist').strip())
    }
    db.close()
    return data


def select_banks_api():
    db = Database(db_name='./api/banks.db')
    query = db.select_banks_db('banks')
    data = []
    banks = {}
    for bank in query:
        data.append({'code': str(bank[0]).strip(), 'bank': str(bank[1]).strip()})
        banks.update(data)
    db.close()
    return data
