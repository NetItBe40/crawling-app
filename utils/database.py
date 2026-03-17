"""
Database connection pool manager using mysql-connector-python
"""
import logging
import time
from contextlib import contextmanager
from mysql.connector import pooling, Error as MySQLError
from config.settings import DB_CONFIG

logger = logging.getLogger(__name__)


class DatabaseManager:
    """Manages connection pools for crawling and SIRENE databases."""

    _pools = {}

    @classmethod
    def get_pool(cls, db_name: str = "crawling") -> pooling.MySQLConnectionPool:
        if db_name not in cls._pools:
            config = DB_CONFIG[db_name].copy()
            pool_name = config.pop("pool_name", f"{db_name}_pool")
            pool_size = config.pop("pool_size", 5)
            try:
                cls._pools[db_name] = pooling.MySQLConnectionPool(
                    pool_name=pool_name,
                    pool_size=pool_size,
                    pool_reset_session=True,
                    **config,
                )
                logger.info(f"Connection pool '{pool_name}' created (size={pool_size})")
            except MySQLError as e:
                logger.error(f"Failed to create pool '{pool_name}': {e}")
                raise
        return cls._pools[db_name]

    @classmethod
    @contextmanager
    def get_connection(cls, db_name: str = "crawling"):
        """Context manager that yields a database connection from the pool."""
        pool = cls.get_pool(db_name)
        conn = None
        try:
            conn = pool.get_connection()
            yield conn
        except MySQLError as e:
            logger.error(f"Database error on '{db_name}': {e}")
            raise
        finally:
            if conn and conn.is_connected():
                conn.close()

    @classmethod
    @contextmanager
    def get_cursor(cls, db_name: str = "crawling", dictionary: bool = True, buffered: bool = True):
        """Context manager that yields a cursor from the pool."""
        with cls.get_connection(db_name) as conn:
            cursor = conn.cursor(dictionary=dictionary, buffered=buffered)
            try:
                yield cursor
                conn.commit()
            except MySQLError as e:
                conn.rollback()
                logger.error(f"Query error on '{db_name}': {e}")
                raise
            finally:
                cursor.close()

    @classmethod
    def execute_many(cls, query: str, data: list, db_name: str = "crawling", batch_size: int = 1000):
        """Execute a query with many rows, in batches."""
        total = len(data)
        inserted = 0
        with cls.get_connection(db_name) as conn:
            cursor = conn.cursor()
            try:
                for i in range(0, total, batch_size):
                    batch = data[i:i + batch_size]
                    cursor.executemany(query, batch)
                    conn.commit()
                    inserted += len(batch)
                logger.debug(f"Inserted {inserted}/{total} rows")
            except MySQLError as e:
                conn.rollback()
                logger.error(f"Batch insert error: {e}")
                raise
            finally:
                cursor.close()
        return inserted

    @classmethod
    def execute_query(cls, query: str, params: tuple = None, db_name: str = "crawling",
                      dictionary: bool = True, fetch: str = "all"):
        """Execute a single query and return results with retry on connection errors."""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with cls.get_cursor(db_name, dictionary=dictionary) as cursor:
                    cursor.execute(query, params)
                    if fetch == "all":
                        return cursor.fetchall()
                    elif fetch == "one":
                        return cursor.fetchone()
                    elif fetch == "none":
                        return cursor.rowcount
                    return cursor.fetchall()
            except MySQLError as e:
                if attempt < max_retries - 1 and e.errno in (2002, 2003, 2006, 2013, 2055):
                    logger.warning(f"Query retry {attempt+1}/{max_retries} on '{db_name}': {e}")
                    if db_name in cls._pools:
                        del cls._pools[db_name]
                    time.sleep(1 * (2 ** attempt))
                else:
                    raise

    @classmethod
    def close_all(cls):
        """Close all connection pools."""
        for name, pool in cls._pools.items():
            logger.info(f"Closing pool '{name}'")
        cls._pools.clear()
