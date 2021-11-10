import os
import time
import pymssql


try:
    DBHOST = os.environ['DBHOST']
    DBUSER = os.environ['DBUSER']
    DBPASSWD = os.environ['DBPASSWD']
    DATABASE = os.environ['DATABASE']
except KeyError:
    DBHOST, DBUSER, DBPASSWD, DATABASE = None, None, None, None


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

    def __iter__(self):
        return iter(v for v in self._data.values())

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

    def __iter__(self):
        return iter(self._rows)

    def __len__(self):
        return len(self._rows)

    # __getitem__ uses []
    def __getitem__(self, key: int):
        return self._rows[key]

    def __str__(self) -> str:
        return str([str(row) for row in self._rows])


def _chunker(seq: list, size: int):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


class Database:

    def __init__(self, server: str = DBHOST, port: int = 1433, database: str = DATABASE, username: str = DBUSER, password: str = DBPASSWD, charset='UTF-8'):
        self._connection = pymssql.connect(server=server, user=username, password=password, database=database, charset=charset, port=port)
        self._cursor = self._connection.cursor(as_dict=True)

    def __enter__(self):
        return self

    def __exit__(self, _type, _value, _traceback):
        self.close()

    def query(self, sqlQuery: str, params: list = None):
        """
Parameterizes query and runs in the database, returning the results. Replaces ?'s in query with params in the proper order. Use
for SELECT and EXEC if the stored procedure is meant to return something.
        :param sqlQuery: The query to run
        :param params: A list of parameter values to substitute for ?'s in the query
        """
        if params:
            new_sql_query = sqlQuery.replace('?', '%s')
        else:
            new_sql_query = sqlQuery

        cursor = self._connection.cursor(as_dict=True)

        if params:
            cursor.execute(new_sql_query, tuple(params))
        else:
            cursor.execute(sqlQuery)
        res = cursor.fetchall()
        cursor.close()
        return Response(res)

    def execute_proc(self, procCode: str, params: list, commit: bool = False, convert_blanks_to_nulls: bool = True, timeout_seconds: int = 200):
        """
Run a stored procedure in the database and wait for it to finish
        :param convert_blanks_to_nulls: Uses NULL as a substitute for blank strings ("") in the parameters
        :param procCode: The code to run the stored procedure
        :param params: Any parameters ya might want
        :param commit: Commits after running if True
        :param timeout_seconds: The max number of seconds to wait for the stored procedure to finish execution
        """
        cursor = self._connection.cursor()

        if params:
            if convert_blanks_to_nulls:
                params = [p if p != "" else None for p in params]
            cursor.execute(procCode.replace('?', '%s'), tuple(params))
        else:
            cursor.execute(procCode)

        # Wait to finish
        slept_times = 0
        while cursor.nextset():
            if (slept_times / 2) >= timeout_seconds:
                break
            time.sleep(0.5)
            slept_times += 1

        cursor.close()
        if commit:
            self._connection.commit()

    def execute_stmt(self, sqlStmt: str, params: list = None, commit: bool = False, convert_blanks_to_nulls: bool = True):
        """
Parameterizes statement and runs in the database. Use for INSERT, UPDATE, DROP, and EXEC commands where no results are expected to be returned.
        :param sqlStmt: The code to run
        :param params: Any parameters, substitute for question marks ('?') in the sqlStmt
        :param commit: Commits the changes in the database if True
        :param convert_blanks_to_nulls: Specifies whether or not to convert blank strings to NULL when parameterizing
        """
        if params:
            new_sql_stmt = sqlStmt.replace('?', '%s')
        else:
            new_sql_stmt = sqlStmt

        if params:
            if convert_blanks_to_nulls:
                params = [p if p != "" else None for p in params]
            self._cursor.execute(new_sql_stmt, tuple(params))
        else:
            self._cursor.execute(new_sql_stmt)

        if commit:
            self._connection.commit()

    def execute_many(self, sqlStmt: str, multipleParams: list, fast: bool = False, commit: bool = False, convert_blanks_to_nulls: bool = True):
        cursor = self._connection.cursor()
        statements = []

        for param_set in multipleParams:
            statements.append(PreparedStatement(sql=sqlStmt, params=param_set, convert_blanks_to_nulls=convert_blanks_to_nulls).sql)

        if fast:
            cursor.execute(";".join(statements))
        else:
            # If we are not doing it 'fast', we will split it up into batches of commands
            for command_chunk in _chunker(statements, 100):
                # statement =
                # logging.info(f'SK.py->: Our SQL statement is: \n\n{statement}\n\n')
                cursor.execute(';'.join(command_chunk))

        cursor.close()
        if commit:
            self._connection.commit()

    def close(self):
        self._connection.commit()
        self._connection.close()


class ParameterMismatchError(Exception):
    pass


class PreparedStatement:
    """
    Please please PLEASE only use this class for debugging. It is NOT secure against SQL injections, since it simply substitutes
    parameter placeholders with whatever value they are assigned, with no data validation.
    DO NOT USE WITH UN-SANITIZED USER DATA!!!
    This class is very useful for debugging queries to see how they look with all their parameters in place. For huge SQL statements,
    performance is much slower than just executing the query with the params using Database.execute_stmt() or Database.query().
    """
    def __init__(self, sql: str, params: list, convert_blanks_to_nulls: bool = True):
        self._sql = sql
        self._params = params
        self._blank_conversion = convert_blanks_to_nulls
        self._finished_sql_statement: str = ''
        self._prepare()

    def _prepare(self):
        if not self._params:
            self._finished_sql_statement = self._sql
            return
        try:
            param_index = 0
            for char in self._sql:
                if char == '?':
                    if type(self._params[param_index]) == str:
                        if self._params[param_index] == '':
                            if self._blank_conversion:
                                self._finished_sql_statement += 'NULL'
                            else:
                                self._finished_sql_statement += "''"
                        else:
                            self._finished_sql_statement += f"""'{self._params[param_index].replace("'", "''")}'"""
                    else:
                        self._finished_sql_statement += str(self._params[param_index])
                    param_index += 1
                else:
                    self._finished_sql_statement += char
        except IndexError:
            raise ParameterMismatchError("The number of parameters in the SQL code did not match the params list")

        self.sql = self.get_finished_sql()

    def get_finished_sql(self):
        return self._finished_sql_statement

    def __str__(self):
        return self.sql
