from sys import version_info
import pymysql
import sqlalchemy as sa

import os

# Secrets for local execution
from env import setenv

engine = None


def handler(event, context):
    global engine
    print("Hello world")
    print("Region:", os.getenv("AWS_REGION"))
    print("Version:", version_info)

    print("Attempting to connect to DB")
    DB_USER = os.getenv("DB_USER")
    print("User:", DB_USER)
    DB_PWD = os.getenv("DB_PWD")
    DB_ENDPOINT = os.getenv("DB_ENDPOINT")
    print("Endpoint:", DB_ENDPOINT)
    DB_INSTANCE_NAME = os.getenv("DB_INSTANCE_NAME")
    print("DB Instance name:", DB_INSTANCE_NAME)
    # conn_string = f"mysql+pymysql://{DB_USER}:{DB_PWD}@{DB_ENDPOINT}:3306/{DB_INSTANCE_NAME}?charset=utf8mb4&unix_socket=/tmp/mysql.sock"
    conn_string = f"mysql+pymysql://{DB_USER}:{DB_PWD}@{DB_ENDPOINT}:3306/{DB_INSTANCE_NAME}?charset=utf8mb4"
    print("Engine:", engine)
    if engine is None:
        print("Creating engine")
        engine = sa.create_engine(conn_string)
        print("Connecting")
        engine.connect()
        print("Connected")
    else:
        print("Already connected, reusing connection")

    print("Fetching version")
    result = engine.execute("SELECT VERSION()")
    for row in result:
        print(row)

    print("Attempting to fetch data")
    result = engine.execute("SELECT * FROM t_workflow")
    print("Statement executed. Result:")
    for row in result:
        print(row)


if __name__ == "__main__":
    setenv()
    handler("", "")
