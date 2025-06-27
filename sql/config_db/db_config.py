import psycopg2

def get_connection():
    return psycopg2.connect(
        host = 'localhost',
        database = 'online_retail',
        user = 'postgres',
        password = 'admin'
    )