from impyla.dbapi import connect
import pandas as pd
import logging

class ImpalaConnector:
    def __init__(self, host, port, user=None, password=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.conn = None

    def connect(self):
        try:
            self.conn = connect(host=self.host, port=self.port, user=self.user, password=self.password, auth_mechanism='PLAIN')
            logging.info('Conexión a Impala exitosa')
        except Exception as e:
            logging.error(f'Error conectando a Impala: {e}')
            raise

    def run_query(self, query):
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        try:
            logging.info(f'Ejecutando query: {query}')
            cursor.execute(query)
        except Exception as e:
            logging.error(f'Error ejecutando query: {e}')
            raise
        finally:
            cursor.close()

    def insert_dataframe(self, df, table, batch_size=10000):
        if not self.conn:
            self.connect()
        cursor = self.conn.cursor()
        cols = ','.join(df.columns)
        n = len(df)
        for i in range(0, n, batch_size):
            batch = df.iloc[i:i+batch_size]
            values = ','.join(['(' + ','.join([repr(x) for x in row]) + ')' for row in batch.values])
            insert_query = f'INSERT INTO {table} ({cols}) VALUES {values}'
            try:
                logging.info(f'Insertando registros {i+1}-{min(i+batch_size, n)} de {n}')
                cursor.execute(insert_query)
            except Exception as e:
                logging.error(f'Error insertando datos: {e}')
                raise
        cursor.close() 