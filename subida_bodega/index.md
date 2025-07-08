import sys
sys.path.append('./src')

from impala_con.subida_bodega import subir_datos_datalake, Impala

# Para usar la función de carga:
subir_datos_datalake(
    dsn = "DSN=impalanube",
    csv_path_or_dataframe = data_preventiva,
    table_name = "te_respaldamos.cob1jlu_base_preventiva",
    batch_size = 10000
)

# Para usar la clase Impala directamente:
impala = Impala("DSN=impalanube")
impala.run_sql_query("TRUNCATE TABLE IF EXISTS te_respaldamos.cob1jlu_base_preventiva")


import sys
sys.path.append('./src')

from impala_con.subida_bodega import subir_datos_datalake, Impala

# Para usar la función de carga:
subir_datos_datalake(
    dsn = "DSN=impalanube",
    csv_path_or_dataframe = data_preventiva,
    table_name = "te_respaldamos.cob1jlu_base_preventiva",
    batch_size = 10000
)

# Para usar la clase Impala directamente:
impala = Impala("DSN=impalanube")
impala.run_sql_query("TRUNCATE TABLE IF EXISTS te_respaldamos.cob1jlu_base_preventiva")