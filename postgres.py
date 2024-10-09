"""
_summary_: Python code to create AWS RDS Postgres instance
"""

import psycopg2
# from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import boto3
import time

rds_client = boto3.client('rds')

def create_db_instance():
    """
    _summary_: Create AWS RDS Postgres instance

    Returns:
        _type_: dict
    """
    response = rds_client.create_db_instance(
        DBName = 'cdc_python_aws_etl',
        DBInstanceIdentifier = 'cdc-python-aws-etl',
        AllocatedStorage = 10,
        DBInstanceClass = 'db.t3.micro',
        Engine = 'postgres',
        MasterUsername = 'guest',
        MasterUserPassword = '8xr6DN_*gKVPRkPx2ssF',
        PubliclyAccessible = True,
        DBSubnetGroupName = 'cdc-python-aws-etl',
        VpcSecurityGroupIds = [
            'sg-01a978662f6fade85'
        ]
    )

    # Wait until the instance is created
    print("Creating RDS instance. This may take a while...")
    waiter = rds_client.get_waiter('db_instance_available')
    waiter.wait(DBInstanceIdentifier='cdc-python-aws-etl')

    print("RDS instance created.")

    return response

def get_db_endpoint(db_instance_identifier):
    """
    _summary_: Get the endpoint of the AWS RDS Postgres instance

    Args:
        db_instance_identifier (_type_): Provide the DB instance identifier

    Returns:
        _type_: str
    """
    response = rds_client.describe_db_instances(
        DBInstanceIdentifier=db_instance_identifier,
    )

    return response['DBInstances'][0]['Endpoint']['Address']

def connect_db_instance(host):
    """
    _summary_: Connect to AWS RDS Postgres instance

    Returns:
        _type_: dict
    """
    # conn = None
    try:
        print('Connecting to the PostgreSQL database...')
        # Add a delay to ensure RDS is fully ready
        time.sleep(15)
        conn = psycopg2.connect(
            # host='cdc-python-aws-etl.c8p7tpsuvmq9.eu-west-1.rds.amazonaws.com',
            host=host,
            port=5432,
            dbname='cdc_python_aws_etl',
            user='guest',
            password='8xr6DN_*gKVPRkPx2ssF',
        )

        # conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        print('Connected to the PostgreSQL database.')
        conn.close()
        print('Database connection closed.')

        # cur = conn.cursor()
        # create = cur.execute('CREATE DATABASE temp;')
        # if create is None:
        #     print('Database created.')
        # else:
        #     print('Database not created.')

        # conn.commit()

    except (Exception, psycopg2.DatabaseError) as e:
        print(e)
    # finally:
    #     if conn is not None:
    #         conn.close()
    #         print('Database connection closed.')

db = create_db_instance()
pre_endpoint = db['DBInstance']['DBInstanceIdentifier']
endpoint = get_db_endpoint(db_instance_identifier=pre_endpoint)
connect_db_instance(host='cdc-python-aws-etl.c8p7tpsuvmq9.eu-west-1.rds.amazonaws.com')
