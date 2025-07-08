import pandas as pd
import logging
from config.settings import EXCEL_PATH, SQL_TABLE, BATCH_SIZE, IMPALA_HOST, IMPALA_PORT, IMPALA_USER, IMPALA_PASSWORD
from connectors.impala_connector import ImpalaConnector
from utils.validators import validate_columns

REQUIRED_COLUMNS = [
    'fuente', 'Producto 2', 'mes', 'Agru. Ciclo', 'Fecha Seguimiento',
    'Cuentas Norm', 'Saldo Norm', 'Cuentas Pendientes', 'Saldo Pendiente',
    'Momentos', 'Saldo Norm x Dia'
]

def run_etl():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    logging.info('Iniciando pipeline ETL de Excel a SQL')
    # Leer Excel
    try:
        df = pd.read_excel(EXCEL_PATH)
        logging.info(f'Archivo Excel leído: {EXCEL_PATH}')
    except Exception as e:
        logging.error(f'Error leyendo el archivo Excel: {e}')
        return
    # Validar columnas
    try:
        validate_columns(df, REQUIRED_COLUMNS)
        logging.info('Columnas validadas correctamente')
    except Exception as e:
        logging.error(f'Error en validación de columnas: {e}')
        return
    # Conectar a Impala
    impala = ImpalaConnector(IMPALA_HOST, IMPALA_PORT, IMPALA_USER, IMPALA_PASSWORD)
    try:
        impala.connect()
    except Exception as e:
        logging.error('No se pudo conectar a Impala')
        return
    # Truncar tabla destino
    try:
        impala.run_query(f'TRUNCATE TABLE IF EXISTS {SQL_TABLE}')
        logging.info(f'Tabla {SQL_TABLE} truncada')
    except Exception as e:
        logging.error(f'No se pudo truncar la tabla: {e}')
        return
    # Insertar datos
    try:
        impala.insert_dataframe(df, SQL_TABLE, batch_size=BATCH_SIZE)
        logging.info('Datos insertados correctamente')
    except Exception as e:
        logging.error(f'Error insertando datos: {e}')
        return
    logging.info('Pipeline ETL finalizado') 