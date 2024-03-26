import longjrm.load_env as jrm_env
from longjrm.connection.dbconn import DatabaseConnection


database = {'mysql': 'mysql-test',
            'postgres': 'postgres-test',
            'mongodb': 'mongodb-test'
            }

dbtype = 'mysql'

dbinfo = jrm_env.dbinfos[database[dbtype]]
db_connection = DatabaseConnection(dbinfo)

try:
    conn = db_connection.connect()
    if dbtype != 'mongodb':
        with conn.cursor() as cursor:
            # Read a single record
            sql = "SELECT * from sample"
            cursor.execute(sql)
            result = cursor.fetchone()
            print(result)
    else:
        # database name has to be defined for the mongodb connection
        result1 = conn[dbinfo['database']]['Listing'].find_one()
        print(result1)
        result2 = []
        where = {'guestCount': 4, 'roomCount': 2}
        select_query = {'filter': where, 'limit': 1}
        res_cur = conn[dbinfo['database']]['Listing'].find(**select_query)
        for row in res_cur:
            result2.append(row)
        print(result2)
except Exception as e:
    print(e)
