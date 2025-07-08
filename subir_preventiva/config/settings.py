import os
from dotenv import load_dotenv

load_dotenv()

IMPALA_HOST = os.getenv('IMPALA_HOST', 'impalanube')
IMPALA_PORT = int(os.getenv('IMPALA_PORT', 21050))
IMPALA_USER = os.getenv('IMPALA_USER', None)
IMPALA_PASSWORD = os.getenv('IMPALA_PASSWORD', None)
EXCEL_PATH = os.getenv('EXCEL_PATH', None)
SQL_TABLE = os.getenv('SQL_TABLE', 'te_respaldamos.cob1jlu_base_preventiva')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', 10000)) 