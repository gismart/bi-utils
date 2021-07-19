import json
import boto3
import psycopg2
import sqlalchemy as sa
from typing import Optional

from ..logger import get_logger
from .locopy import locopy


logger = get_logger(__name__)
cached_creds = {}


def get_creds(secret_id: str = 'prod/redshift/analytics') -> dict:
    '''Get AWS credentials'''
    creds = cached_creds.get(secret_id)
    if not creds:
        client = boto3.client('secretsmanager')
        secret = client.get_secret_value(SecretId=secret_id)
        creds = json.loads(secret['SecretString'])
        cached_creds[secret_id] = creds
        logger.info(f'Loaded AWS credentials ({secret_id})')
    return creds


def get_redshift(
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
) -> locopy.Redshift:
    '''Get locopy redshift connection'''
    creds = get_creds(secret_id)
    dbname = database or creds.get('dbname')
    redshift = locopy.Redshift(
        dbapi=psycopg2,
        host=creds.get('host'),
        port=creds.get('port'),
        dbname=dbname,
        user=creds.get('username'),
        password=creds.get('password'),
    )
    logger.info(f'Created {dbname} RedShift connection')
    return redshift


def create_engine(
    secret_id: str = 'prod/redshift/analytics',
    drivername: str = 'postgresql+psycopg2',
    database: Optional[str] = None,
) -> sa.engine.Engine:
    '''Create AWS connection engine'''
    creds = get_creds(secret_id=secret_id)
    dbname = database or creds.get('dbname')
    conn_str = sa.engine.url.URL(
        host=creds.get('host'),
        port=creds.get('port'),
        database=dbname,
        username=creds.get('username'),
        password=creds.get('password'),
        drivername=drivername,
    )
    engine = sa.create_engine(conn_str)
    logger.info(f'Created {dbname} DB engine')
    return engine


def connect(
    schema: Optional[str] = None,
    secret_id: str = 'prod/redshift/analytics',
    database: Optional[str] = None,
) -> psycopg2.extensions.connection:
    '''Connect to db via psycopg2'''
    creds = get_creds(secret_id=secret_id)
    dbname = database or creds.get('dbname')
    conn = psycopg2.connect(
        host=creds.get('host'),
        port=creds.get('port'),
        dbname=dbname,
        user=creds.get('username'),
        password=creds.get('password'),
        options=f'--search_path={schema}' if schema else None,
    )
    schema_postfix = f'{schema} schema' if schema else ''
    logger.info(f'Connected to {dbname} DB {schema_postfix}')
    return conn
