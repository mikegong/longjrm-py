from longjrm.connection.pool import Pools
from longjrm.database.db import Db


database = {'mysql': 'mysql-test',
            'postgres': 'postgres-test',
            'mongodb': 'mongodb-test'
            }

dbtype = 'mysql'

pools = Pools()
pools.start_pool(database[dbtype])
client = pools.get_client(database[dbtype])

db = Db(client)

# result = db.query("SELECT VERSION()")

if dbtype == 'mongodb':
    result = db.select(table='Listing', where={'guestCount': 4, 'roomCount': 2})
    print(result)
else:
    result1 = db.select(table='sample', columns=['*'], where={'c1': 'a'})
    result2 = db.select(table='sample', columns=['*'], where={'c2': 3})
    print(result1)
    print(result2)

# cursor = conn.cursor()
# cursor.execute("SELECT VERSION()")
# version = cursor.fetchone()
# print("Database version:", version)
# Make sure to close the connection

pools.release_connection(client)
