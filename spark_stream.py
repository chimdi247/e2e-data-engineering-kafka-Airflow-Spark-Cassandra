import logging
import os
import uuid

import pyspark
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Where Kafka and Cassandra are reachable from the machine running this script.
# On your laptop the defaults work, because docker-compose.yml publishes Kafka on
# 9092 and Cassandra on 9042. If you run the script inside the Docker network,
# set KAFKA_BOOTSTRAP_SERVERS=broker:29092 and CASSANDRA_HOST=cassandra instead.
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
CASSANDRA_HOST = os.getenv('CASSANDRA_HOST', 'localhost')

# The Kafka connector has to match the Spark version exactly (Spark 4.x is Scala 2.13 only).
# Using pyspark.__version__ keeps it in step with the installed pyspark (4.2.0 here,
# matching the Spark 4.2.0 cluster in docker-compose.yml).
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"

CASSANDRA_COLUMNS = ['id', 'first_name', 'last_name', 'gender', 'address', 'post_code',
                     'email', 'username', 'registered_date', 'phone', 'picture']
INSERT_CQL = (f"INSERT INTO spark_streams.created_users ({', '.join(CASSANDRA_COLUMNS)}) "
              f"VALUES ({', '.join(['?'] * len(CASSANDRA_COLUMNS))})")


def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)

    print("Keyspace created successfully!")


def create_table(session):
    session.execute("""
    CREATE TABLE IF NOT EXISTS spark_streams.created_users (
        id UUID PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        address TEXT,
        post_code TEXT,
        email TEXT,
        username TEXT,
        registered_date TEXT,
        phone TEXT,
        picture TEXT);
    """)

    print("Table created successfully!")


def insert_data(session, **kwargs):
    print("inserting data...")

    user_id = kwargs.get('id')
    first_name = kwargs.get('first_name')
    last_name = kwargs.get('last_name')
    gender = kwargs.get('gender')
    address = kwargs.get('address')
    postcode = kwargs.get('post_code')
    email = kwargs.get('email')
    username = kwargs.get('username')
    dob = kwargs.get('dob')
    registered_date = kwargs.get('registered_date')
    phone = kwargs.get('phone')
    picture = kwargs.get('picture')

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address,
                post_code, email, username, dob, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (user_id, first_name, last_name, gender, address,
              postcode, email, username, dob, registered_date, phone, picture))
        logging.info(f"Data inserted for {first_name} {last_name}")

    except Exception as e:
        logging.error(f'could not insert data due to {e}')


def create_spark_connection():
    s_conn = None

    try:
        # Only the Kafka connector is needed now. The DataStax spark-cassandra-connector
        # (latest 3.5.1) is built for Spark 3.5 and has no Spark 4 release, so the
        # Cassandra writes below go through the Python cassandra-driver instead.
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', KAFKA_PACKAGE) \
            .getOrCreate()

        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', KAFKA_BOOTSTRAP_SERVERS) \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"kafka dataframe could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    try:
        # connecting to the cassandra cluster
        cluster = Cluster([CASSANDRA_HOST])

        cas_session = cluster.connect()

        return cas_session
    except Exception as e:
        logging.error(f"Could not create cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    print(sel)

    return sel


def make_cassandra_batch_writer(session):
    """Build the function Spark calls for every micro-batch.

    Replaces the old 'org.apache.spark.sql.cassandra' sink. Rows are pulled to the
    driver and inserted with the Python cassandra-driver, which is fine for the small
    volumes this pipeline handles. Inserts are keyed on id, so a retried batch just
    overwrites the same rows.
    """
    insert = session.prepare(INSERT_CQL)

    def write_batch(batch_df, batch_id):
        params = []
        for row in batch_df.toLocalIterator():
            if row['id'] is None:
                continue
            params.append((uuid.UUID(row['id']),) + tuple(row[c] for c in CASSANDRA_COLUMNS[1:]))

        if not params:
            return

        results = execute_concurrent_with_args(session, insert, params,
                                               concurrency=50, raise_on_first_error=False)
        failed = [err for ok, err in results if not ok]
        if failed:
            # Raising makes Spark retry the batch from the checkpoint.
            raise RuntimeError(f"{len(failed)} of {len(params)} rows failed in batch {batch_id}: {failed[0]}")

        logging.info(f"Batch {batch_id}: wrote {len(params)} rows to Cassandra")

    return write_batch


if __name__ == "__main__":
    # create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            logging.info("Streaming is being started...")

            streaming_query = (selection_df.writeStream
                               .foreachBatch(make_cassandra_batch_writer(session))
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .start())

            streaming_query.awaitTermination()
