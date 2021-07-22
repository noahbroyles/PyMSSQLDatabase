import time
import pymssql


class Iter(type):
    def __iter__(self):
        # Wanna iterate over a class? Then ask that damn class for an iterator!
        return self.classiter()


class Row:
    def __init__(self, data: dict):
        self._data: dict = data
        self._listed_data = [[k, v] for k, v in self._data.items()]

    def get(self, key):
        return self.__getattr__(key)

    def __getitem__(self, item):
        return self._listed_data[item][1]

    def __setitem__(self, key, value):
        self._listed_data[key][1] = value
        self._data = {t[0]: t[1] for t in self._listed_data}

    @classmethod
    def classiter(cls):
        return iter(v for v in cls._data.values())

    def __len__(self):
        return len(self._data)

    def __getattr__(self, key):
        return self._data.get(key)

    def __str__(self) -> str:
        return str(self._data)


class Response:
    _rows = None

    def __init__(self, response: list):
        self._base_data: list = response
        self._rows = [Row(d) for d in self._base_data]

    @classmethod
    def classiter(cls):
        return iter(cls._rows)

    def __len__(self):
        return len(self._rows)

    # __getitem__ uses []
    def __getitem__(self, key: int):
        return self._rows[key]

    def __str__(self) -> str:
        return str([str(row) for row in self._rows])


class Database:

    def __init__(self, server: str, database: str, username: str, password: str):

        self._conn = pymssql.connect(server, username, password, database)
        self._cursor = self._conn.cursor(as_dict=True)

    def query(self, sqlQuery: str, params: list = None):
        """
Parameterizes query and runs in the database, returning the results. Replaces ?'s in query with params in the proper order. Use
for SELECT and EXEC if the stored procedure is meant to return something.
        :param sqlQuery: The query to run
        :param params: A list of parameter values to substitute for ?'s in the query
        """
        if params:
            new_sql_query = ''
            for i in range(len(sqlQuery)):
                if sqlQuery[i] == '?':
                    new_sql_query += '%s'
                else:
                    new_sql_query += sqlQuery[i]
        else:
            new_sql_query = sqlQuery

        cursor = self._conn.cursor(as_dict=True)

        if params:
            cursor.execute(new_sql_query, tuple(params))
        else:
            cursor.execute(sqlQuery)
        res = cursor.fetchall()
        cursor.close()
        return Response(res)

    def execute(self, sqlStmt: str, params: list = None, commit: bool = True, timeout_seconds=100):
        """
Parameterizes statement and runs in the database. Use for INSERT, UPDATE, DROP, and EXEC commands where no results are expected to be returned.
        :param commit: Commits the changes in the database if True
        :param sqlStmt: The code to run
        :param params: Any parameters, substitute for question marks ('?') in the sqlStmt
        """
        if params:
            new_sql_stmt = ''
            for i in range(len(sqlStmt)):
                if sqlStmt[i] == '?':
                    new_sql_stmt += '%s'
                else:
                    new_sql_stmt += sqlStmt[i]
        else:
            new_sql_stmt = sqlStmt

        # with open('C:/bestnest_com/web/3PL/query.sql', 'w') as qf:
        #     qf.write(newSqlStmt)

        if params:
            self._cursor.execute(new_sql_stmt, *params)
        else:
            self._cursor.execute(new_sql_stmt)

        slept_times = 0
        while self._cursor.nextset():
            if (slept_times / 10) >= timeout_seconds:
                break
            time.sleep(0.1)
            slept_times += 1

        if commit:
            self._conn.commit()

    def close(self):
        self._conn.commit()
        self._conn.close()
