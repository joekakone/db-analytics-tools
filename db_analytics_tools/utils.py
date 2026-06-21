# coding : utf-8


"""
    DB Analytics Tools : Utils

    This module provides classes and functions for database interactions and data migration.
"""


#####################################################################################################
# Package Imports
#####################################################################################################
import urllib
import datetime
import json

import pandas as pd
from sqlalchemy import create_engine, text
#####################################################################################################


# Frequeny
FREQ = {
    "Hourly": "h",
    "Daily": "d",
    "Weekly": "w",
    "Monthly": "m"
}


class Client:
    """
    SQL Client Class

    This class provides a client for connecting to SQL databases such as PostgreSQL and SQL Server.

    :param host: The hostname or IP address of the database server.
    :param port: The port number to use for the database connection.
    :param database: The name of the database to connect to.
    :param username: The username for authenticating the database connection.
    :param password: The password for authenticating the database connection.
    :param engine: The database engine to use, currently supports 'postgres' and 'mssql'.
    :param keep_connection: If True, the connection will be maintained until explicitly closed. If False, the connection
                           will be opened and closed for each database operation (default is False).
    """

    def __init__(self, host, port, database, username, password, engine="postgres", keep_connection=False):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.engine = engine
        self.keep_connection = keep_connection
        # Test Connection #
        self.test_connection()
        # Generate URI #
        self.generate_uri()

    def connect(self, verbose=0):
        """
        Establish a connection to the database.

        :param verbose: If set to 1, print a success message upon connection.
        """
        if self.engine in ("postgres", "greenplum"):
            import psycopg2
            self.conn = psycopg2.connect(host=self.host,
                                         port=self.port,
                                         database=self.database,
                                         user=self.username,
                                         password=self.password)
            note = 'Connection established successfully !'
        elif self.engine == "mssql":
            import pyodbc
            
            # Detect available ODBC drivers for SQL Server
            drivers = [d for d in pyodbc.drivers() if "ODBC Driver" in d and "SQL Server" in d]
            if not drivers:
                raise ValueError("No compatible ODBC driver found for SQL Server.")
            
            # Try to connect using each available driver until a successful connection is established
            for i, driver in enumerate(drivers):
                try:
                    self.conn = pyodbc.connect(
                        f"DRIVER={{{driver}}};"
                        f"SERVER={self.host},{self.port};"
                        f"DATABASE={self.database};"
                        f"UID={self.username};"
                        f"PWD={self.password};"
                        f"TrustServerCertificate=yes;"
                    )
                    self.driver = driver
                    note = f'Successfully connected using driver: {driver}'
                    break  # Connection successful, exit the loop
                except Exception as e:
                # except pyodbc.Error as e:
                    # If this is the last available driver and it fails, raise the error
                    if i == len(drivers) - 1:
                        raise RuntimeError(f"Failed to connect to SQL Server with available drivers. Last error: {e}")
        else:
            raise NotImplementedError("Engine not supported")
        
        # Create cursor
        self.cursor = self.conn.cursor()
        
        if verbose == 1:
            print(note)

    def test_connection(self, verbose=1):
        """
        Test the database connection.

        :param verbose: If set to 1, print a success message upon successful connection.
        :raises Exception: If the connection test fails.
        """
        try:
            self.connect(verbose=verbose)
            if not self.keep_connection:
                self.close()
        except Exception:
            raise Exception("Something went wrong! Verify database info and credentials.")

    def close(self, verbose=0):
        """
        Close the database connection.

        :param verbose: If set to 1, print a success message upon closing the connection.
        """
        self.cursor.close()
        self.conn.close()
        
        if verbose == 1:
            print('Connection closed successfully!')

    def generate_uri(self):
        """
        Generate a connection URI based on the provided parameters.
        """
        password = urllib.parse.quote(self.password)
        
        if self.engine in ("postgres", "greenplum"):
            self.uri = f"postgresql+psycopg2://{self.username}:{password}@{self.host}:{self.port}/{self.database}"
            
            engine = create_engine(self.uri)
            self.conn = engine.connect().execution_options(stream_results=True)

        elif self.engine == "mssql":
            uri = f"mssql+pyodbc://{self.username}:{password}@{self.host}:{self.port}/{self.database}"

            engine = create_engine(
                uri,
                connect_args={
                    "driver": f"{{{self.driver}}}",
                    "TrustServerCertificate": "yes"
                }
            )
            self.conn = engine.connect().execution_options(stream_results=True)
            self.uri = engine
        else:
            raise NotImplementedError("Engine not supported")

    def execute(self, query, verbose=0):
        """
        Execute an SQL query.

        :param query: The SQL query to execute.
        :param verbose: If set to 1, print the execution time.
        """
        start_time = datetime.datetime.now()

        if not self.keep_connection:
            self.connect()
        self.cursor.execute(query)
        self.conn.commit()
        if not self.keep_connection:
            self.close()
        duration = datetime.datetime.now() - start_time
        
        if verbose == 1:
            print(f'Execution time: {duration}')

    def read_sql(self, query, chunksize=None, verbose=0):
        """
        Execute an SQL query and return the result as a Pandas DataFrame.

        :param query: The SQL query to execute.
        :param chunksize: If specified, return an iterator where chunksize is the number of rows to include in each chunk.
        :param verbose: If set to 1, print the execution time.
        :return: A Pandas DataFrame containing the query result.
        """
        start_time = datetime.datetime.now()

        if chunksize:
            dataframe = pd.read_sql(sql=text(query), con=self.conn, chunksize=chunksize)
        else:
            dataframe = pd.read_sql(sql=query, con=self.uri)
        duration = datetime.datetime.now() - start_time
        
        if verbose == 1:
            print(f'Execution time: {duration}')

        return dataframe

    def grant(self, table_name, username, level, verbose=0):
        """
        Grant specific privileges on a database table to a user.

        :param table_name: The name of the database table on which to grant privileges.
        :param username: The username of the user to whom privileges will be granted.
        :param level: The level of privileges to grant, which can be one of: 'select', 'update', 'insert', 'delete', 'all'.
        :param verbose: If set to 1, print the execution time.
        """
        levels = ("select", "update", "insert", "delete", "all")
        try:
            assert level in levels
        except Exception as e:
            print(f"Not support value for level '{level}'. Excepted {levels}")
        
        mapping = {
            "select": "select",
            "update": "update",
            "insert": "insert",
            "delete": "delete",
            "all": "all privileges"
        }
        query = "grant " + mapping[level] + " on " + table_name + " to " + username + ";"

        start_time = datetime.datetime.now()

        if not self.keep_connection:
            self.connect()
        self.cursor.execute(query)
        self.conn.commit()
        if not self.keep_connection:
            self.close()
        duration = datetime.datetime.now() - start_time
        
        if verbose == 1:
            print(f'Execution time: {duration}')

    def revoke(self, table_name, username, level, verbose=0):
        """
        Revoke specific privileges on a database table from a user.

        :param table_name: The name of the database table on which to revoke privileges.
        :param username: The username of the user from whom privileges will be revoked.
        :param level: The level of privileges to revoke, which can be one of: 'select', 'update', 'insert', 'delete', 'all'.
        :param verbose: If set to 1, print the execution time.
        """
        assert level in ("select", "update", "insert", "delete", "all")
        mapping = {
            "select": "select",
            "update": "update",
            "insert": "insert",
            "delete": "delete",
            "all": "all privileges"
        }
        query = "revoke " + mapping[level] + " from " + username + " on " + table_name + ";"

        start_time = datetime.datetime.now()

        if not self.keep_connection:
            self.connect()
        self.cursor.execute(query)
        self.conn.commit()
        if not self.keep_connection:
            self.close()
        duration = datetime.datetime.now() - start_time
        
        if verbose == 1:
            print(f'Execution time: {duration}')
            
    def sample_table(self, table_name, n=100):
        """
        Retrieve a sample of rows from a specified database table.

        :param table_name: The name of the database table from which to retrieve a sample.
        :param n: The number of rows to include in the sample (default is 100).
        :return: A Pandas DataFrame containing the sampled rows from the specified table.
        """
        if self.engine in ("postgres", "greenplum"):
            query = f"select * from {table_name} limit {n};"
        elif self.engine == "mssql":
            query = f"SELECT TOP({n}) * FROM {table_name};"
        else:
            raise NotImplementedError("Engine not supported")
        
        return self.read_sql(query)

    def get_tables(self, include_all=False, include_size=False):
        """
        Retrieves and displays the list of tables in the connected database.

        This method queries the database to fetch the names of all tables, along with their schema and size information.

        - For PostgreSQL/Greenplum, it retrieves table information from `pg_catalog.pg_tables` and calculates table sizes.
        - For SQL Server, it retrieves table information from `sys.tables`, map partitions metadata, and matches the identical column schema layout.

        :raises NotImplementedError: If the database engine is not supported.
        :return: A DataFrame containing table details.
        """
        expected_columns = [
            'schemaname', 'tablename', 'full_tablename', 
            'tableowner', 'size', 'size_bytes', 
            'partition_count', 'min_partition', 'max_partition', 'drop_query'
        ]
        
        try:
            if self.engine in ("postgres", "greenplum"):
                query = f"""
                    with _sq as (
                        select a.relid, b.tableowner, b.schemaname, b.tablename, {'pg_relation_size(a.relid)' if include_size else 'null'} size_bytes
                        from pg_catalog.pg_statio_user_tables a
                                join pg_catalog.pg_tables b
                                    on a.relname = b.tablename and a.schemaname = b.schemaname
                        where 1 = 1
                        and {'b.tableowner = current_user' if not include_all else '1 = 1'}
                        and tablename not like '%%_prt_%%' -- Ignore partitions
                    ),
                        _partitions as (
                            select parent.relname,
                                    count(*)                                                                partition_count,
                                    min(case when child.relname not like '%%extra%%' then child.relname end) min_partition,
                                    max(case when child.relname not like '%%extra%%' then child.relname end) max_partition
                            from pg_inherits
                                    join pg_class parent on pg_inherits.inhparent = parent.oid
                                    join pg_class child on pg_inherits.inhrelid = child.oid
                            group by 1
                        )
                    select schemaname,
                        tablename,
                        schemaname || '.' || tablename                                                      full_tablename,
                        tableowner,
                        {'pg_size_pretty(size_bytes)' if include_size else 'null'}        size,
                        {'size_bytes::int8' if include_size else 'null'}                  size_bytes,
                        coalesce(partition_count, 0)                                                     partition_count,
                        coalesce(min_partition, tablename)                                              min_partition,
                        coalesce(max_partition, tablename)                                              max_partition,
                        'drop table if exists ' || schemaname || '.' || tablename || ';' drop_query
                    from _sq a
                            left join _partitions b on a.tablename = b.relname
                    order by partition_count desc, schemaname, tablename;
                """
            elif self.engine == "mssql":
                # Rebuilt SQL Server query to output exact match for the Pandas DataFrame structure expected by UI components
                query = f"""
                    WITH _raw_size AS (
                        SELECT 
                            t.object_id,
                            SUM(p.reserved_page_count) * 8 * 1024 AS size_bytes
                        FROM sys.tables t
                        JOIN sys.dm_db_partition_stats p ON t.object_id = p.object_id
                        GROUP BY t.object_id
                    ),
                    _partitions AS (
                        SELECT 
                            p.object_id,
                            COUNT(DISTINCT p.partition_number) AS partition_count,
                            -- SQL Server uses partition numbers rather than sub-table names
                            'partition_1' AS min_partition,
                            'partition_' + CAST(COUNT(DISTINCT p.partition_number) AS VARCHAR(10)) AS max_partition
                        FROM sys.partitions p
                        GROUP BY p.object_id
                    )
                    SELECT 
                        s.name AS schemaname,
                        t.name AS tablename,
                        s.name + '.' + t.name AS full_tablename,
                        -- USER_NAME() maps to the schema or DB principal owner context
                        COALESCE(pnl.name, USER_NAME(t.principal_id), USER_NAME(s.principal_id), 'dbo') AS tableowner,
                        {'''
                        CASE 
                            WHEN sz.size_bytes >= 1073741824 THEN CAST(CAST(sz.size_bytes / 1073741824.0 AS DECIMAL(10,2)) AS VARCHAR(20)) + ' GB'
                            WHEN sz.size_bytes >= 1048576 THEN CAST(CAST(sz.size_bytes / 1048576.0 AS DECIMAL(10,2)) AS VARCHAR(20)) + ' MB'
                            WHEN sz.size_bytes >= 1024 THEN CAST(CAST(sz.size_bytes / 1024.0 AS DECIMAL(10,2)) AS VARCHAR(20)) + ' KB'
                            ELSE CAST(sz.size_bytes AS VARCHAR(20)) + ' bytes'
                        END
                        ''' if include_size else 'NULL'} AS size,
                        { 'CAST(sz.size_bytes AS BIGINT)' if include_size else 'NULL' } AS size_bytes,
                        -- If partition_count <= 1, SQL Server treats it as a standard heap/B-tree (0 for UI match)
                        CASE WHEN pt.partition_count > 1 THEN pt.partition_count ELSE 0 END AS partition_count,
                        CASE WHEN pt.partition_count > 1 THEN pt.min_partition ELSE t.name END AS min_partition,
                        CASE WHEN pt.partition_count > 1 THEN pt.max_partition ELSE t.name END AS max_partition,
                        'DROP TABLE IF EXISTS ' + s.name + '.' + t.name + ';' AS drop_query
                    FROM sys.tables t
                    JOIN sys.schemas s ON t.schema_id = s.schema_id
                    LEFT JOIN sys.database_principals pnl ON t.principal_id = pnl.principal_id
                    LEFT JOIN _raw_size sz ON t.object_id = sz.object_id
                    LEFT JOIN _partitions pt ON t.object_id = pt.object_id
                    WHERE 1 = 1
                    { "AND (USER_NAME(t.principal_id) = USER_NAME() OR USER_NAME(s.principal_id) = USER_NAME() OR s.name = 'dbo')" if not include_all else "" }
                    ORDER BY partition_count DESC, schemaname, tablename;
                """
            else:
                raise NotImplementedError("Engine not supported")
        except Exception as e:
            print(f"Error occurred while fetching datas from db: {e}")
            return pd.DataFrame(columns=expected_columns)
            
        return self.read_sql(query)

    def get_views(self, include_all=False):
        """
        Retrieves and displays the list of views in the connected database.

        This method queries the database to fetch the metadata of all views, 
        matching identical schema outputs across different relational engines.

        - For PostgreSQL/Greenplum, it retrieves view definitions from `pg_catalog.pg_views`.
        - For SQL Server, it maps system views via `sys.views` and extracts source definitions.

        :raises NotImplementedError: If the database engine is not supported.
        :return: A DataFrame containing view details.
        """
        expected_columns = [
            'schemaname', 'viewname', 'full_viewname', 
            'viewowner', 'definition'
        ]
        
        try:
            if self.engine in ("postgres", "greenplum"):
                query = f"""
                    select schemaname,
                        viewname,
                        schemaname || '.' || viewname full_viewname,
                        viewowner,
                        definition
                    from pg_catalog.pg_views
                    where schemaname not in ('pg_catalog', 'information_schema') -- Exclude system views
                        and {'viewowner = current_user' if not include_all else '1 = 1'}
                    order by schemaname, viewname
                """
            elif self.engine == "mssql":
                # Rebuilt SQL Server block to target views metadata and output the exact Postgres-like column names
                query = f"""
                    SELECT 
                        s.name AS schemaname,
                        v.name AS viewname,
                        s.name + '.' + v.name AS full_viewname,
                        COALESCE(pnl.name, USER_NAME(v.principal_id), USER_NAME(s.principal_id), 'dbo') AS viewowner,
                        m.definition AS definition
                    FROM sys.views v
                    JOIN sys.schemas s ON v.schema_id = s.schema_id
                    LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
                    LEFT JOIN sys.database_principals pnl ON v.principal_id = pnl.principal_id
                    WHERE s.name NOT IN ('sys', 'information_schema') -- Exclude core system schemas
                    { "AND (USER_NAME(v.principal_id) = USER_NAME() OR USER_NAME(s.principal_id) = USER_NAME() OR s.name = 'dbo')" if not include_all else "" }
                    ORDER BY schemaname, viewname;
                """
            else:
                raise NotImplementedError("Engine not supported")
        except Exception as e:
            print(f"Error occurred while fetching datas from db: {e}")
            return pd.DataFrame(columns=expected_columns)

        return self.read_sql(query)

    def get_functions(self, include_all=False):
        """
        Retrieves and displays the list of functions in the connected database.

        This method queries the database to fetch the names of all functions, along with their schema and size information.

        - For PostgreSQL, it retrieves table information from `pg_catalog.pg_proc`.
        - For SQL Server, it retrieves table information from `sys.tables` and calculates table sizes using `sys.dm_db_partition_stats`.

        :raises NotImplementedError: If the database engine is not supported.
        :return: A DataFrame containing table details.
        """
        expected_columns = [
            'schemaname', 'functionname', 'full_functionname', 
            'functionowner', 'result_type', 'argument_types', 
            'type', 'language', 'source_code'
        ]
        
        try:
            if self.engine == "postgres":
                query = f"""
                    select n.nspname                        schemaname,
                        p.proname                        functionname,
                        n.nspname || '.' || p.proname    full_functionname,
                        pg_get_userbyid(p.proowner)      functionowner,
                        pg_get_function_result(p.oid)    result_type,
                        pg_get_function_arguments(p.oid) argument_types,
                        case
                            when p.prokind = 'f' then 'function'
                            when p.prokind = 'p' then 'procedure'
                            when p.prokind = 'a' then 'aggregate'
                            when p.prokind = 'w' then 'window'
                            end                          type,
                        l.lanname                        language,
                        p.prosrc                         source_code
                    from pg_catalog.pg_proc p
                            left join pg_catalog.pg_namespace n on n.oid = p.pronamespace
                            left join pg_catalog.pg_language l on l.oid = p.prolang
                    where n.nspname not in ('pg_catalog', 'information_schema')
                        and {'pg_get_userbyid(p.proowner) = current_user' if not include_all else '1 = 1'}
                    order by schemaname, functionname
                """
            elif self.engine == "greenplum":
                query = f"""
                    select n.nspname                        schemaname,
                        p.proname                        functionname,
                        n.nspname || '.' || p.proname    full_functionname,
                        pg_get_userbyid(p.proowner)      functionowner,
                        pg_get_function_result(p.oid)    result_type,
                        pg_get_function_arguments(p.oid) argument_types,
                        case
                            when p.proisagg then 'aggregate'
                            when p.proiswindow then 'window'
                            else 'function'
                            end                          type,
                        l.lanname                        language,
                        p.prosrc                         source_code
                    from pg_catalog.pg_proc p
                            left join pg_catalog.pg_namespace n on n.oid = p.pronamespace
                            left join pg_catalog.pg_language l on l.oid = p.prolang
                    where n.nspname not in ('pg_catalog', 'information_schema')
                        and {'pg_get_userbyid(p.proowner) = current_user' if not include_all else '1 = 1'}
                    order by schemaname, functionname
                """
            elif self.engine == "mssql":
                # Rebuilt SQL Server block to fetch routines metadata matching Postgres schema exactly
                filter_user = """(CASE 
                            WHEN COALESCE(pnl.name, USER_NAME(o.principal_id), USER_NAME(s.principal_id)) = 'dbo' 
                            THEN (SELECT suser_sname(owner_sid) FROM sys.databases WHERE name = DB_NAME())
                            ELSE COALESCE(pnl.name, USER_NAME(o.principal_id), USER_NAME(s.principal_id), 'dbo')
                        END = SUSER_NAME())
                """ if not include_all else "1 = 1"
                query = f"""
                    SELECT 
                        s.name AS schemaname,
                        o.name AS functionname,
                        s.name + '.' + o.name AS full_functionname,
                        CASE 
                            WHEN COALESCE(pnl.name, USER_NAME(o.principal_id), USER_NAME(s.principal_id)) = 'dbo' 
                            THEN (SELECT suser_sname(owner_sid) FROM sys.databases WHERE name = DB_NAME())
                            ELSE COALESCE(pnl.name, USER_NAME(o.principal_id), USER_NAME(s.principal_id), 'dbo')
                        END AS functionowner,
                        COALESCE(CAST(t.name AS VARCHAR(50)), 'void') AS result_type,
                        NULL AS argument_types,
                        CASE 
                            WHEN o.type IN ('P', 'PC') THEN 'procedure'
                            WHEN o.type IN ('FN', 'IF', 'TF', 'FS', 'FT') THEN 'function'
                            ELSE 'function'
                        END AS type,
                        'TSQL' AS language,
                        m.definition AS source_code
                    FROM sys.objects o
                    JOIN sys.schemas s ON o.schema_id = s.schema_id
                    LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
                    LEFT JOIN sys.database_principals pnl ON o.principal_id = pnl.principal_id
                    LEFT JOIN sys.parameters p ON o.object_id = p.object_id AND p.parameter_id = 0
                    LEFT JOIN sys.types t ON p.system_type_id = t.system_type_id AND p.user_type_id = t.user_type_id
                    WHERE o.type IN ('P', 'FN', 'IF', 'TF', 'FS', 'FT')
                    AND s.name NOT IN ('sys', 'information_schema')
                    AND {filter_user}
                    ORDER BY schemaname, functionname;
                """
            else:
                raise NotImplementedError("Engine not supported")
        except Exception as e:
            print(f"Error occurred while fetching datas from db: {e}")
            return pd.DataFrame(columns=expected_columns)

        return self.read_sql(query)

    def get_roles(self, include_groups=False):
        """
        Retrieves and displays the list of roles in the connected database.

        This method queries the database to fetch the names of all roles, along with their schema and size information.

        - For PostgreSQL, it retrieves table information from `pg_catalog.pg_roles`.
        - For SQL Server, it retrieves table information from `sys.tables` and calculates table sizes using `sys.dm_db_partition_stats`.

        :raises NotImplementedError: If the database engine is not supported.
        :return: A DataFrame containing table details.
        """
        expected_columns = [
            'rolename', 'is_superuser', 'can_create_role', 'can_create_db', 'is_user', 
            'is_replication_role', 'connection_limit', 'valid_until', 'member_of'
        ]
        
        try:
            if self.engine in ("postgres", "greenplum"):
                query = f"""
                    select rolname        rolename,
                        rolsuper       is_superuser,
                        rolcreaterole  can_create_role,
                        rolcreatedb    can_create_db,
                        rolcanlogin    is_user,
                        rolreplication is_replication_role,
                        rolconnlimit   connection_limit,
                        rolvaliduntil  valid_until,
                        array(
                                select b.rolname
                                from pg_catalog.pg_auth_members m
                                            join pg_catalog.pg_roles b on m.roleid = b.oid
                                where m.member = r.oid
                            )          member_of
                    from pg_catalog.pg_roles r
                    where 1 = 1
                        and {'rolcanlogin' if not include_groups else '1 = 1'}
                    order by is_user desc, rolename
                """
            elif self.engine == "mssql":
                # Maps database principals, users, permissions, and roles hierarchies 
                query = f"""
                    WITH _roles_members AS (
                        SELECT 
                            m.principal_id AS member_id,
                            r.name AS role_name
                        FROM sys.database_role_members rm
                        JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
                        JOIN sys.database_principals m ON rm.member_principal_id = m.principal_id
                    )
                    SELECT 
                        dp.name AS rolename,
                        CAST(dp.is_fixed_role AS BIT) AS is_superuser,
                        CAST(0 AS BIT) AS can_create_role,
                        CAST(0 AS BIT) AS can_create_db,
                        CASE WHEN dp.type IN ('S', 'U', 'G') THEN CAST(1 AS BIT) ELSE CAST(0 AS BIT) END AS is_user,
                        CAST(0 AS BIT) AS is_replication_role,
                        NULL AS connection_limit,
                        NULL AS valid_until,
                        (
                            SELECT rm.role_name + ','
                            FROM _roles_members rm
                            WHERE rm.member_id = dp.principal_id
                            FOR XML PATH('')
                        ) AS member_of
                    FROM sys.database_principals dp
                    WHERE dp.name NOT IN ('public', 'INFORMATION_SCHEMA', 'sys')
                    { "AND dp.type IN ('S', 'U', 'G')" if not include_groups else "" }
                    ORDER BY is_user DESC, rolename;
                """
            else:
                raise NotImplementedError("Engine not supported")
        except Exception as e:
            print(f"Error occurred while fetching datas from db: {e}")
            return pd.DataFrame(columns=expected_columns)

        return self.read_sql(query)

    def show_sessions(self, include_all=False):
        """
        Retrieves and displays the list of active database sessions for the current user.

        This method queries the database to fetch session details, including session ID, 
        user name, client address, application name, query status, and timestamps.

        - For PostgreSQL, it retrieves detailed session information from `pg_catalog.pg_stat_activity`.
        - For SQL Server, it retrieves running requests from `sys.dm_exec_requests`.

        :raises NotImplementedError: If the database engine is not supported.
        :return: A DataFrame containing session details.
        """
        expected_columns = [
            'session_id', 'resource_group_id', 'session_internal_id', 'username', 'client_address',
            'application_name', 'query', 'state', 'waiting', 'waiting_reason', 'waiting_time_ms',
            'query_start', 'backend_start', 'xact_start', 'state_change', 'cpu_time',
            'total_elapsed_time', 'reads_', 'writes_'
        ]
        
        try:
            if self.engine == "postgres":
                query = f"""
                    select 
                        pid                                              session_id,
                        null                                             resource_group_id,
                        null                                             session_internal_id,
                        usename                                          username,
                        client_addr                                      client_address,
                        application_name,
                        query,
                        state,
                        wait_event is not null                           waiting,
                        wait_event                                       waiting_reason,
                        null                                             waiting_time_ms,
                        query_start,
                        backend_start,
                        xact_start,
                        state_change,
                        null                                             cpu_time,
                        extract(epoch from (now() - query_start)) * 1000 total_elapsed_time,
                        null                                             reads_,
                        null                                             writes_
                    from pg_catalog.pg_stat_activity
                    where 1 = 1
                        and {'usename = current_user' if not include_all else '1 = 1'}
                    order by query_start desc;
                """
            elif self.engine == "greenplum":
                query = f"""
                    select 
                        pid                                              session_id,
                        rsgid                                            resource_group_id,
                        sess_id                                          session_internal_id,
                        usename                                          username,
                        client_addr                                      client_address,
                        application_name,
                        query,
                        state,
                        waiting,
                        waiting_reason,
                        null                                             waiting_time_ms,
                        query_start,
                        backend_start,
                        xact_start,
                        state_change,
                        null                                             cpu_time,
                        extract(epoch from (now() - query_start)) * 1000 total_elapsed_time,
                        null                                             reads_,
                        null                                             writes_
                    from pg_catalog.pg_stat_activity
                    where 1 = 1
                        and {'usename = current_user' if not include_all else '1 = 1'}
                    order by query_start desc;
                """
            elif self.engine == "mssql":
                query = f"""
                    SELECT 
                        s.session_id                                             AS session_id,
                        NULL                                                     AS resource_group_id,
                        r.request_id                                             AS session_internal_id,
                        s.login_name                                             AS username,
                        s.host_name                                              AS client_address,
                        s.program_name                                           AS application_name,
                        -- Fixed: Dynamic extraction of the real textual sql handle query string
                        COALESCE(st.text, r.command)                             AS query,
                        s.status                                                 AS state,
                        CAST(CASE WHEN r.wait_time = 0 THEN 0 ELSE 1 END AS BIT) AS waiting,
                        r.wait_type                                              AS waiting_reason,
                        r.wait_time                                              AS waiting_time_ms,
                        r.start_time                                             AS query_start,
                        s.login_time                                             AS backend_start,
                        NULL                                                     AS xact_start,
                        NULL                                                     AS state_change,
                        r.cpu_time                                               AS cpu_time,
                        r.total_elapsed_time                                     AS total_elapsed_time,
                        r.reads                                                  AS reads_,
                        r.writes                                                 AS writes_
                    FROM sys.dm_exec_sessions s
                    LEFT JOIN sys.dm_exec_requests r ON s.session_id = r.session_id
                    OUTER APPLY sys.dm_exec_sql_text(r.sql_handle) st
                    WHERE s.is_user_process = 1
                        { "AND s.login_name = SUSER_NAME()" if not include_all else "" }
                    ORDER BY r.start_time DESC;
                """
            else:
                raise NotImplementedError("Engine not supported")
        except Exception as e:
            print(f"Error occurred while fetching datas from db: {e}")
            return pd.DataFrame(columns=expected_columns)
        
        return self.read_sql(query)

    def cancel_all_queries(self, verbose=0):
        """
        Cancel all running queries.

        :param verbose: If set to 1, print the execution time.
        """
        if self.engine in ("postgres", "greenplum"):
            query = "select pg_terminate_backend(pid) from pg_catalog.pg_stat_activity where usename = current_user;"
        elif self.engine == "mssql":
            query = "kill (select session_id from sys.dm_exec_sessions where login_name = SUSER_NAME());"
        else:
            raise NotImplementedError("Engine not supported")

        self.execute(query, verbose=verbose)

    def cancel_query(self, session_id, verbose=0):
        """
        Cancel a running query based on its session ID.

        :param session_id: The session ID of the query to cancel.
        :param verbose: If set to 1, print the execution time.
        """
        if self.engine in ("postgres", "greenplum"):
            query = f"select pg_terminate_backend({session_id});"
        elif self.engine == "mssql":
            query = f"kill {session_id};"
        else:
            raise NotImplementedError("Engine not supported")

        self.execute(query, verbose=verbose)

    def cancel_locked_queries(self, verbose=0):
        """
        Cancel a locked queries.

        :param verbose: If set to 1, print the execution time.
        """
        locks = self.show_sessions().query("waiting == True").to_dict(orient="records")
        for lock in locks:
            print(f"Canceling session ID: {lock['session_id']} ... '{lock['query'][:25]}'", end=" ... ")
            self.cancel_query(lock["session_id"], verbose=verbose)
            print("Canceled !")


def create_client(host, port, database, username, password, engine, keep_connection):
    """
    Creates a SQL Client instance for database operations.

    This function establishes a connection to the specified database server with the provided
    credentials and engine type. It returns a `Client` instance which can be used to interact with
    the database, allowing data extraction, transformation, and loading (ETL) operations.

    :param host: str - The hostname or IP address of the database server.
    :param port: int - The port number to use for the database connection.
    :param database: str - The name of the database to connect to.
    :param username: str - The username for authenticating the database connection.
    :param password: str - The password for authenticating the database connection.
    :param engine: str - The database engine to use; supported values are 'postgres' and 'mssql'.
    :param keep_connection: bool - If True, the connection will be maintained until explicitly closed.
                            If False, the connection will be opened and closed for each operation.
    :return: Client - A `Client` instance configured for interacting with the specified database.
    """
    client = Client(host=host,
                    port=port,
                    database=database,
                    username=username,
                    password=password,
                    engine=engine,
                    keep_connection=keep_connection)
    return client


def create_client_from_config(config):
    """
    Creates a SQL Client instance from a configuration dictionary.

    This function takes a configuration dictionary containing the necessary connection details
    and creates a `Client` instance for performing data operations. The configuration dictionary
    should contain the following keys: 'host', 'port', 'database', 'username', 'password',
    'engine', and 'keep_connection'.

    :param config: dict - A dictionary containing the database connection parameters:
                   - host: str - The hostname or IP address of the database server.
                   - port: int - The port number to use for the database connection.
                   - database: str - The name of the database to connect to.
                   - username: str - The username for authenticating the database connection.
                   - password: str - The password for authenticating the database connection.
                   - engine: str - The database engine to use; supported values are 'postgres' and 'mssql'.
                   - keep_connection: bool - If True, the connection will be maintained until explicitly closed.
                                              If False, the connection will be opened and closed for each operation.
    :return: Client - A `Client` instance configured for interacting with the specified database.
    """
    client = Client(host=config.get("host"),
                    port=config.get("port"),
                    database=config.get("database"),
                    username=config.get("username"),
                    password=config.get("password"),
                    engine=config.get("engine"),
                    keep_connection=config.get("keep_connection", False))
    return client


def truncate_table(db_client, table_name, if_exists):
    """
    Truncate a database table.

    This function removes all rows from the specified table in the database, effectively resetting it.

    :param db_client: An instance of the `Client` class for connecting to the database.
    :param table_name: The name of the table to truncate, in the format 'schema_name.table_name'.
    """
    
    if if_exists == "truncate":
        if db_client.engine in ("postgres", "greenplum"):
            sql = f"TRUNCATE TABLE {table_name};"
        elif db_client.engine == "mssql":
            sql = f"TRUNCATE TABLE {table_name};"
        else:
            raise NotImplementedError("Engine not supported for truncate operation")
        
        try:
            db_client.execute(query=sql)
        except Exception as e: # If table does not exist or other error
            raise Exception(f"Failed to truncate table {table_name}, maybe table doesn't exist: {e}")
        
        # After truncation, set if_exists to "append"
        if_exists = "append"

    return if_exists


def dataframe_to_csv(dataframe, output_file, sep=";", encoding='latin_1', mode='w', verbose=0):
    """
    Save a Pandas DataFrame to a CSV file.

    :param dataframe: The Pandas DataFrame to be saved.
    :param output_file: The path to the output CSV file.
    :param sep: The delimiter to use between fields in the CSV file.
    :param encoding: The character encoding for the CSV file (default is 'latin_1').
    """
    
    start_time = datetime.datetime.now()
    
    dataframe.to_csv(output_file,
                     sep=sep,
                     encoding=encoding,
                     index=False,
                     mode=mode)

    duration = datetime.datetime.now() - start_time
    if verbose == 1:
        print(f'Execution time: {duration}')


def db_to_csv(query, db_client, output_file, sep=";", encoding='latin_1', chunksize=None, verbose=0):
    """
    Execute a SQL query and save the result to a CSV file.

    :param query: The SQL query to be executed.
    :param db_client: An instance of the `Client` class for connecting to the database.
    :param output_file: The path to the output CSV file.
    :param sep: The delimiter to use between fields in the CSV file.
    :param encoding: The character encoding for the CSV file (default is 'latin_1').
    """

    start_time = datetime.datetime.now()
    total_rows = 0

    if chunksize:
        chunks = db_client.read_sql(query, chunksize=chunksize)

        for i, chunk in enumerate(chunks):
            mode = "w" if i == 0 else "a"
            dataframe_to_csv(dataframe=chunk,
                            output_file=output_file,
                            sep=sep,
                            encoding=encoding,
                            mode=mode)

            total_rows += len(chunk)
            
            duration = datetime.datetime.now() - start_time
            if verbose == 1:
                print(f"Step : {str(i).rjust(3, '0')}...\tProgression : {total_rows:,} lines...\tElapsed : {duration}")

            # Free memory
            del chunk
    else:
        dataframe = db_client.read_sql(query)
        dataframe_to_csv(dataframe=dataframe,
                         output_file=output_file,
                         sep=sep,
                         encoding=encoding)

    duration = datetime.datetime.now() - start_time
    if verbose == 1:
        print(f'Execution time: {duration}')


def dataframe_to_db(dataframe, db_client, destination_table, if_exists="append", chunksize=None, verbose=0):
    """
    Load data from a Pandas DataFrame to a database table.

    :param dataframe: Pandas DataFrame containing the data to be loaded.
    :param db_client: Database client.
    :param destination_table: Destination table in the format 'schema_name.table_name'.
    :param if_exists: Action to take if the table already exists ('fail', 'replace', or 'append').
    :param chunksize: Number of rows to insert in each batch (default is 10000).
    """
    if "." in destination_table:
        schema_name, table_name = destination_table.rsplit(".", 1)
    else:
        schema_name, table_name = None, destination_table
    
    start_time = datetime.datetime.now()
    
    if_exists = truncate_table(db_client, destination_table, if_exists)

    dataframe.to_sql(name=table_name,
                     schema=schema_name,
                     con=db_client.uri,
                     index=False,
                     chunksize=chunksize,
                     method="multi",
                     if_exists=if_exists)

    duration = datetime.datetime.now() - start_time
    if verbose == 1:
        print(f'Execution time: {duration}')


def csv_to_db(input_file, db_client, destination_table, sep=";", if_exists="append", chunksize=None, verbose=0):
    """
    Load data from a CSV file to a database table.

    :param input_file: Path to the input CSV file.
    :param db_client: Database client.
    :param destination_table: Destination table in the format 'schema_name.table_name'.
    :param sep: Separator character for the CSV file (default is ';').
    :param if_exists: Action to take if the table already exists ('fail', 'replace', or 'append').
    :param chunksize: Number of rows to insert in each batch (default is 10000).
    """
    
    start_time = datetime.datetime.now()

    dataframe = pd.read_csv(input_file, sep=sep)
    
    # if_exists == "truncate"
    if_exists = truncate_table(db_client, destination_table, if_exists)

    dataframe_to_db(dataframe=dataframe,
                    db_client=db_client,
                    destination_table=destination_table,
                    if_exists=if_exists,
                    chunksize=chunksize)

    duration = datetime.datetime.now() - start_time
    if verbose == 1:
        print(f'Execution time: {duration}')


def db_to_db(query, source_client, destination_client, destination_table, if_exists="append", chunksize=None, verbose=0):
    """
    Transfer data from one database to another using an SQL query.

    :param query: SQL query to retrieve data from the source database.
    :param source_client: Source database client.
    :param destination_client: Destination database client.
    :param destination_table: Destination table in the format 'schema_name.table_name'.
    :param if_exists: Action to take if the destination table already exists ('fail', 'replace', or 'append').
    :param chunksize: Number of rows to insert in each batch (default is 10000).
    :param verbose: If set to 1, print the execution time.
    """

    start_time = datetime.datetime.now()
    total_rows = 0

    if chunksize:
        chunks = source_client.read_sql(query, chunksize=chunksize)

        for i, chunk in enumerate(chunks):
            current_if_exists = if_exists if i == 0 else "append"
            dataframe_to_db(dataframe=chunk,
                            db_client=destination_client,
                            destination_table=destination_table,
                            if_exists=current_if_exists,
                            chunksize=chunksize)

            total_rows += len(chunk)
            
            duration = datetime.datetime.now() - start_time
            if verbose == 1:
                print(f"Step : {str(i).rjust(3, '0')}...\tProgression : {total_rows:,} lines...\tElapsed : {duration}")

            # Free memory
            del chunk
    else:
        dataframe = source_client.read_sql(query)

        # if_exists == "truncate"
        if_exists = truncate_table(destination_client, destination_table, if_exists)

        dataframe_to_db(dataframe=dataframe,
                        db_client=destination_client,
                        destination_table=destination_table,
                        if_exists=if_exists,
                        chunksize=chunksize)

    duration = datetime.datetime.now() - start_time
    if verbose == 1:
        print(f'Execution time: {duration}')


def get_config(path):
    """
    Load configuration from a config file

    :param path: Path config file path
    :return: dict
    """
    with open(path, "r") as f:
        config = json.load(f)

    return config
