import boto3
import psycopg2
import logging
import os
import etl

NEWYORK_DATA = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
JOHN_DATA = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"

sns = boto3.client('sns')
s3 = boto3.client("s3")
db_table = os.environ['DB_TABLE']

# Setup logging
logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)  # To see output in local console
logger.setLevel(logging.INFO)  # To see output in Lambda

def notify(text):
    try:
        sns.publish(
            TopicArn = os.environ['TOPIC_ARN'],
            Message = text,
            Subject = 'CDC Python AWS ETL'
        )
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    logger.info(text)

def lambda_handler(event, context):

    # Call the extract_transform function
    try:
        merged_data = etl.extract_transform(NEWYORK_DATA, JOHN_DATA, '/tmp/merged_covid_data.csv')
        logger.info(f"Merged data columns: {merged_data.columns}")
        logger.info(f"Sample of merged data: {merged_data.head()}")


    except Exception as e:
        notify(f"Transformation of data failed: {e}")
        exit(1)

    # Connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(
            host='cdc-python-aws-etl.c8p7tpsuvmq9.eu-west-1.rds.amazonaws.com',
            port=5432,
            dbname='cdc_python_aws_etl',
            user='guest',
            password='8xr6DN_*gKVPRkPx2ssF',
        )

        cur = conn.cursor()
    except (Exception, psycopg2.DatabaseError) as e:
        notify(f"Database connection failed: {e}")
        exit(1)

    # Create table if it does not exist
    try:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {db_table} (report_date date PRIMARY KEY, cases int, deaths int, recovered int)")
        cur.execute(f"select COUNT(*) AS num_rows FROM {db_table}")
        rows = cur.fetchone()
        logger.info(f"Number of rows in table: {rows[0]}")
    except (Exception, psycopg2.DatabaseError) as e:
        notify(f"Unable to find info about db table: {e}")
        exit(1)

     # If db table is empty, insert data for the first time
    if rows[0] == 0:
        try:
            with open('merged_covid_data.csv', 'r') as f:
                next(f)
                cur.copy_from(f, db_table, sep=',')
                conn.commit()

                cur.execute(f"select COUNT(*) AS num_rows FROM {db_table}")
                rows = cur.fetchone()
                text = f"First time data insertion successful. Number of rows in table {db_table}: {rows[0]}"
                notify(text)
        except (Exception, psycopg2.DatabaseError) as e:
            notify(f"Data insertion failed: {e}")
            exit(1)
    # If db table is not empty, load new data records into db table
    else:
        cur.execute(f"SELECT MAX(report_date) FROM {db_table}")
        last_date = cur.fetchone()
        logger.info(f"Last date in db table: {last_date[0]}")

        # Filter out records that are greater than the last date in the db table
        diff = max(merged_data['date']).date() - last_date[0]
        logger.info(f"Num of rows different between merged dataset and db table/Number of days to load: {diff.days}")

        if diff.days > 0:
            try:
                logger.info("Uploading new data records into db table..")

                # Clone table structure of destination table to a temp table
                cur.execute(f"CREATE TABLE temp AS TABLE {db_table} WITH NO DATA")

                # insert data into temp table
                with open('merged_covid_data.csv', 'r') as f:
                    next(f)
                    cur.copy_from(f, 'temp', sep=',')
                    conn.commit()

                # Copy new records present in temp table to db table based on missing reportdate rows
                cur.execute(f"INSERT INTO {db_table} SELECT * FROM temp WHERE report_date > '{last_date[0]}'")

                # Drop the temporary table
                cur.execute("DROP TABLE temp")

                # Send the inserted rows to email
                text = f"\nNum of rows inserted into db table {db_table}: {diff.days}"
                cur.execute(f"SELECT * FROM {db_table} ORDER BY reportdate desc LIMIT {diff.days}")
                rows = cur.fetchall()
                text += f"""\n----------------------------------------\nReport_Date | Cases | Deaths | Recovered\n----------------------------------------\n"""
                for row in rows:
                    # Convert each row/column value to string and store the entire row as a tuple
                    row = (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
                    # To concatenate tuple to string, you have to first convert it to a string
                    text += " ".join(row)
                    text += "\n"
                notify(text)

            except (Exception, psycopg2.DatabaseError) as e:
                notify(f"Data insertion failed: {e}")
                exit(1)
        else:
            notify("No new data records to load into db table.")

    # Close the database connection
    cur.close()
    print("Database connection closed.")
