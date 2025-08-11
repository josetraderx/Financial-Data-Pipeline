"""
Setup test database for pytest.
"""

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def setup_test_db():
    """Create test database if it doesn't exist."""
    # Conectar a postgres para crear la base de datos
    conn = psycopg2.connect(
        host="localhost", port=5433, database="postgres", user="postgres", password="Jireh2023."
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Crear la base de datos de prueba
    try:
        cur.execute("CREATE DATABASE exodus_test_db;")
        print("✅ Test database created successfully")
    except psycopg2.Error as e:
        if "already exists" not in str(e):
            raise e
        print("ℹ️ Test database already exists")

    cur.close()
    conn.close()

    # Conectar a la base de datos de prueba para crear la extensión TimescaleDB
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="exodus_test_db",
        user="postgres",
        password="Jireh2023.",
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    try:
        cur.execute("CREATE EXTENSION IF NOT EXISTS timescaledb;")
        print("✅ TimescaleDB extension enabled")
    except psycopg2.Error as e:
        print(f"❌ Error enabling TimescaleDB: {e}")

    cur.close()
    conn.close()


def teardown_test_db():
    """Drop test database after tests."""
    conn = psycopg2.connect(
        host="localhost", port=5433, database="postgres", user="postgres", password="Jireh2023."
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Terminar conexiones existentes
    cur.execute("""
        SELECT pg_terminate_backend(pg_stat_activity.pid)
        FROM pg_stat_activity
        WHERE pg_stat_activity.datname = 'exodus_test_db'
        AND pid <> pg_backend_pid();
    """)

    try:
        cur.execute("DROP DATABASE IF EXISTS exodus_test_db;")
        print("✅ Test database dropped successfully")
    except psycopg2.Error as e:
        print(f"❌ Error dropping database: {e}")

    cur.close()
    conn.close()
