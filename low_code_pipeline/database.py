""" This file contains utility functions used throughout the project """

import logging
import psycopg2
import psycopg2.extras
import requests

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from data_source import DataSource


class Database:
    def __init__(source: DataSource):
        self._conn = psycopg2.connect(dbname=database_name, host=database_host, port=database_port, user=database_user,
                                      password=database_password)
        self.source = source
        if autocommit:
            self._conn.autocommit = True
        self._cursor = self._conn.cursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    @property
    def connection(self):
        return self._conn

    @property
    def cursor(self):
        return self._cursor

    def commit(self):
        self.connection.commit()

    def close(self, commit=True):
        if commit:
            self.commit()
        self.connection.close()

    def execute(self, sql, params=None):
        self.cursor.execute(sql, params or ())

    def fetchall(self):
        return self.cursor.fetchall()

    def fetchone(self):
        return self.cursor.fetchone()

    def query(self, sql, params=None):
        self.cursor.execute(sql, params or ())
        return self.fetchall()


def load_logging_config():
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    return logger


def requests_retry_session(
        retries=3,
        backoff_factor=1,
        session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('https://', adapter)
    return session
