import pymssql


class Iter(type):
    def __iter__(self):
        # Wanna iterate over a class? Then ask that damn class for an iterator!
        return self.classiter()


class Row:
    def __init__(self, data: dict):
        self._data: dict = data
        self._listData = [[k, v] for k, v in self._data.items()]

    def get(self, key):
        return self.__getattr__(key)

    def __getitem__(self, item):
        return self._listData[item][1]

    def __setitem__(self, key, value):
        self._listData[key][1] = value
        self._data = {t[0]: t[1] for t in self._listData}

    @classmethod
    def classiter(cls):
        return iter(v for v in cls._data.values())

    def __len__(self):
        return len(self._data)

    def __getattr__(self, key):
        return self._data.get(key, False)


class Response:
    _rows = None

    def __init__(self, dictResponse: list):
        self._baseDB_data: list = dictResponse
        self._rows = [Row(d) for d in self._baseDB_data]

    @classmethod
    def classiter(cls):
        return iter(cls._rows)

    def __len__(self):
        return len(self._rows)

    # __getitem__ uses []
    def __getitem__(self, key: int):
        return self._rows[key]


class Database:

    def __init__(self, server: str, database: str, dbUsername: str, password: str):

        self._conn = pymssql.connect(server, dbUsername, password, database)
        self._cursor = self._conn.cursor(as_dict=True)

    def query(self, sqlQuery: str, params: list = None):
        """
Parameterizes query and runs in the database, returning the results. Replaces ?'s in query with params in the proper order. Use
for SELECT and EXEC if the stored procedure is meant to return something.
        :param sqlQuery: The query to run
        :param params: A list of parameter values to substitute for ?'s in the query
        """
        if params:
            newSqlQuery = ''
            for i in range(len(sqlQuery)):
                if sqlQuery[i] == '?':
                    newSqlQuery += '%s'
                else:
                    newSqlQuery += sqlQuery[i]
        else:
            newSqlQuery = sqlQuery

        cursor = self._conn.cursor(as_dict=True)

        if params:
            cursor.execute(newSqlQuery, tuple(params))
        else:
            cursor.execute(sqlQuery)
        res = cursor.fetchall()
        cursor.close()
        return Response(res)

    def execute(self, sqlStmt: str, params: list = None, commit: bool = True):
        """
Parameterizes statement and runs in the database. Use for INSERT, UPDATE, DROP, and EXEC commands where no results are expected to be returned.
        :param commit:
        :param sqlStmt:
        :param params:
        """
        if params:
            newSqlStmt = ''
            for i in range(len(sqlStmt)):
                if sqlStmt[i] == '?':
                    newSqlStmt += '%s'
                else:
                    newSqlStmt += sqlStmt[i]
        else:
            newSqlStmt = sqlStmt

        # with open('C:/bestnest_com/web/3PL/query.sql', 'w') as qf:
        #     qf.write(newSqlStmt)

        if params:
            self._cursor.execute(newSqlStmt, *params)
        else:
            self._cursor.execute(newSqlStmt)
        if commit:
            self._conn.commit()

    def close(self):
        self._conn.commit()
        self._conn.close()
        del self
