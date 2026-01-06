import logging
from longjrm.config.config import JrmConfig
from longjrm.connection.dbconn import DatabaseConnection
from longjrm.config.runtime import configure

# Configure logging to output to console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

logger = logging.getLogger(__name__)

#db_key = "postgres-test"
db_key = "mysql-test"


cfg = JrmConfig.from_files("test_config/jrm.config.json", "test_config/dbinfos.json")
# inject the configuration into the runtime
configure(cfg)
db_cfg = cfg.require(db_key)

db_connection = DatabaseConnection(db_cfg)


def setup_sql_sample_table(conn, database_type):
    """Create and populate sample table for SQL databases."""
    with conn.cursor() as cursor:
        # Drop table if exists
        logger.info("Dropping sample table if it exists...")
        cursor.execute("DROP TABLE IF EXISTS sample")
        conn.commit()

        # Create table with appropriate syntax for database type
        logger.info("Creating sample table...")
        if database_type == 'mysql':
            create_sql = """
                CREATE TABLE sample (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        else:  # PostgreSQL
            create_sql = """
                CREATE TABLE sample (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INT,
                    status VARCHAR(20),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        cursor.execute(create_sql)
        conn.commit()

        # Insert sample data
        logger.info("Inserting sample data...")
        sample_data = [
            ("Alice Johnson", "alice@example.com", 28, "active"),
            ("Bob Smith", "bob@example.com", 35, "active"),
            ("Charlie Brown", "charlie@example.com", 42, "inactive"),
            ("Diana Prince", "diana@example.com", 30, "active"),
            ("Eve Wilson", "eve@example.com", 25, "pending")
        ]

        if database_type == 'mysql':
            insert_sql = "INSERT INTO sample (name, email, age, status) VALUES (%s, %s, %s, %s)"
        else:  # PostgreSQL
            insert_sql = "INSERT INTO sample (name, email, age, status) VALUES (%s, %s, %s, %s)"

        cursor.executemany(insert_sql, sample_data)
        conn.commit()
        logger.info(f"✓ Successfully created sample table with {len(sample_data)} records")




try:
    conn = db_connection.connect()

    if db_cfg.type not in ['mongodb', 'mongodb+srv']:
        # SQL databases (MySQL, PostgreSQL)
        logger.info(f"\n{'='*60}")
        logger.info(f"Testing {db_cfg.type.upper()} Connection")
        logger.info(f"{'='*60}\n")

        # Setup sample table
        setup_sql_sample_table(conn, db_cfg.type)

        # Test: Read all records
        logger.info("\nTest 1: Reading all records from sample table...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM sample ORDER BY id")
            results = cursor.fetchall()
            logger.info(f"Found {len(results)} records:")
            for row in results:
                print(f"  {row}")

        # Test: Read single record
        logger.info("\nTest 2: Reading single record...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM sample WHERE status = %s LIMIT 1", ("active",))
            result = cursor.fetchone()
            logger.info(f"Result: {result}")

        # Test: Count records
        logger.info("\nTest 3: Counting active users...")
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM sample WHERE status = %s", ("active",))
            count = cursor.fetchone()
            logger.info(f"Active users: {count[0]}")

        logger.info(f"\n{'='*60}")
        logger.info("✓ All SQL tests completed successfully!")
        logger.info(f"{'='*60}\n")


except Exception as e:
    logger.error(f"\n✗ Test failed with error: {e}")
    import traceback
    traceback.print_exc()
finally:
    db_connection.close()
    logger.info("Connection closed.")
