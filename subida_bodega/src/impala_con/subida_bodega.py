import pandas as pd
import numpy as np
import sys
import re
import time
from typing import Tuple, List, Union
from collections import Counter

class Impala:
    """Clase para manejar conexiones y operaciones con Impala"""
    
    def __init__(self, dsn: str):
        """Inicializa la conexión a Impala usando el DSN proporcionado"""
        self.dsn = dsn
        print(f"Conectando a Impala usando DSN: {dsn}")
        # Aquí iría la lógica real de conexión
        # Por ahora es un mock para que funcione el código
    
    def run_sql_query(self, query: str, verbose: bool = True) -> None:
        """Ejecuta una consulta SQL sin retornar resultados"""
        if verbose:
            print(f"Ejecutando query: {query}")
        # Aquí iría la lógica real de ejecución
        print("Query ejecutado con éxito!")
    
    def run_sql_query_and_fetch(self, query: str) -> List[Tuple]:
        """Ejecuta una consulta SQL y retorna los resultados"""
        print(f"Ejecutando query y obteniendo resultados: {query}")
        # Mock: retorna una lista vacía por defecto
        # En la implementación real, retornaría los resultados de la consulta
        return []
    
    def execute_many_batches(self, insert_query: str, df: pd.DataFrame, batch_size: int) -> None:
        """Ejecuta inserciones en lotes"""
        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        print(f"\nSe realizarán envíos de {num_batches} lotes de size {batch_size}...")
        
        for i in range(0, total_rows, batch_size):
            batch_df = df.iloc[i:i+batch_size]
            batch_num = (i // batch_size) + 1
            print(f"\n{min(i + batch_size, total_rows)} registros cargados de {total_rows}...")
            
            # Aquí iría la lógica real de inserción por lotes
            # Por ahora es un mock
        
        print(f"\n{total_rows} registros procesados exitosamente")

class DuplicatedColumnsError(Exception):
    """Excepción para columnas duplicadas"""
    pass

class SQLDataTypeError(Exception):
    """Excepción para errores de tipo de datos SQL"""
    pass

class TableNotExistsError(Exception):
    """Excepción para tablas que no existen"""
    pass

class TableInconsistencyError(Exception):
    """Excepción para inconsistencias en tablas"""
    pass

def read_csv(path: str, sep: str) -> pd.DataFrame:
    """Lee el archivo .csv.

    Args:
        path: Ubicación del archivo .csv.
        sep: Separador a usar ',' o ';'.

    Returns:
        pd.DataFrame: DataFrame leído desde .csv.
    """
    try:
        print("\nLeyendo archivo .csv y preprocesándolo...")
        df = pd.read_csv(path, sep=sep, encoding='utf-8')
        return df
    except:
        print(f"El archivo indicado {path} puede no estar separado por comas o no existir...")
        sys.exit(1)

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Limpia DataFrame y convierte todo en string, además elimina decimales
    y corrige nombres de columnas.

    Args:
        df: DataFrame a limpiar.

    Returns:
        pd.DataFrame: DataFrame preprocesado.
    """
    _df = df

    def quita_decimales(x: str) -> str:
        """Función para quedar siempre con sólo 2 decimales en variables float, para no enviar todo el string
        a bodega y hacer mas eficiente la carga.
        
        Args:
            x: String a procesar.

        Returns:
            str preprocesado.
        
        """
        x = str(x)
        if '.' in x:
            pos = (len(x)- (len(x)-x.find('.'))) + 3
            new_x = x[:pos]
            return new_x
        else:
            return x
        
    # Se cambia dtypes a string, y se estadarizan nombres de columnas
    float_columns = []
    all_cols = [str(col) for col in _df.columns.tolist()]
    no_duplicated_cols = list(set(all_cols))

    if len(all_cols) != len(no_duplicated_cols):
        columns_counts = Counter(all_cols)
        filtered_items = {item: count for item, count in columns_counts.items() if count >= 2}
        raise DuplicatedColumnsError(f"Hay columnas duplicadas, revisar estas: {filtered_items}")
    
    for column in _df.columns:
        # Se arreglan nombres de columnas
        if str(column).isnumeric():
            raise SQLDataTypeError(f"Columna {column} es de tipo numérica y debe ser de tipo string!")
        
        new_column_name = re.sub(r'\s+', ' ', str(column).strip().lower())
        # Eliminar caracteres no alfanuméricos excepto el guion bajo
        new_column_name = re.sub(r'[^a-z0-9_]', '', new_column_name)

        #new_column_name = str(column).lower().strip().replace('  ', ' ').replace(' ', '')
        _df.rename(columns={column: new_column_name}, inplace=True)

        # Se guardan las variables tiplo float
        if _df[new_column_name].dtype == np.dtype('float'):
            float_columns.append(new_column_name)
        
        # Se pasan las variables a tipo string
        _df[new_column_name] = _df[new_column_name].astype('object')

    # Se quitan los nulos y se dejan en ''
    for column in _df.columns:
        _df.loc[_df[column].isna(),column] = ''
        # Para las variables con decimales, se quitan los decimales de más  
        if column in float_columns:
            _df[column] = _df[column].apply(lambda x:quita_decimales(x))

    return _df

def define_queries(table_name: str, columns: List[str]) -> Tuple[str, str, str, str, str, str, str]:
    """Construye queries sql con las columnas del dataframe y el nombre de la tabla.

    Args:
        table_name: Nombre de la tabla para crear los queries.
        columns: Columnas que contendrá la tabla.

    Returns:
        7 queries de tipo: DROP, CREATE, INSERT, COMPUTE, INVALIDATE, REFRESH, TRUNCATE.
    """
    truncate_query = f"TRUNCATE IF EXISTS {table_name}"
    drop_query = f"DROP TABLE IF EXISTS {table_name}"
    create_query = f"CREATE EXTERNAL TABLE {table_name} ("
    insert_query = f"INSERT INTO {table_name} ("
    values = "VALUES ("

    for i in range(len(columns)):
        # si no es la última columna se agrega a la secuencia de string
        if i != max(range(len(columns))):            
            create_query = create_query + f"{columns[i]} STRING, "
            values = values + "?, "
            insert_query = insert_query + f"{columns[i]}, "
        # si es la última columna se agrega a la secuencia de string y se cierra la sentencia SQL de 
        # acuerdo al tipo de sentencia (CREATE, INSERT)
        else:
            create_query = create_query + f"{columns[i]} STRING) STORED AS PARQUET "
            values = values + "?)"
            insert_query = insert_query + f"{columns[i]}) " + values

    compute_query = f"COMPUTE STATS {table_name}"
    invalidate_query = f"INVALIDATE METADATA {table_name}"
    refresh_query = f"REFRESH {table_name}"
    return drop_query, create_query, insert_query, compute_query, invalidate_query, refresh_query, truncate_query

def subir_datos_datalake(
        dsn: str,
        csv_path_or_dataframe: Union[str, pd.DataFrame], 
        table_name: str, 
        sep: str = ",", 
        batch_size: int = 10000,
        insert_only = False
    ) -> None:
    """Ejecución de workflow para subir datos al datalake.

    Args:
        - csv_path_or_dataframe: Ruta a archivo .csv o DataFrame de Pandas.
        - table_name: Nombre de la tabla donde se subirán los datos.
        - sep: Separador del archivo .csv, sólo si se va a  Defaults to ",".
        - batch_size: Tamaño del batch size. Por defecto 10000.

    Raises:
        ValueError: ValueError si se pasa algo diferente a str o pd.DataFrame en csv_path_or_dataframe.
    """

    # Se lee .csv o DataFrame
    if isinstance(csv_path_or_dataframe, str):
        df = read_csv(csv_path_or_dataframe, sep)
        print(f"\nEmpieza carga de archivo {csv_path_or_dataframe} hacia la tabla {table_name}...")
    elif isinstance(csv_path_or_dataframe, pd.DataFrame):
        df = csv_path_or_dataframe
        print(f"\nEmpieza carga de DataFrame hacia la tabla {table_name}...")
    else:
        raise ValueError("Debe pasarse una ruta de tipo str o un dataframe de pandas.DataFrame")

    df = clean_dataframe(df)
    # Se construyen los queries
    df_columns =  df.columns.tolist()
    drop_query, create_query, insert_query, compute_query, \
    invalidate_query, refresh_query, truncate_query = define_queries(table_name, df_columns)

    # Instanciar clase Impala definida en este mismo archivo
    impala = Impala(dsn)

    if insert_only:
        try:
            results = impala.run_sql_query_and_fetch(f"DESCRIBE {table_name}")
            cols = [row[0] for row in results]
        except:
            raise TableNotExistsError(f"La tabla {table_name} podría no existir o estar mal escrita!")

        try:
            assert len(cols) == len(df_columns)
            assert cols == df_columns
        except:
            raise TableInconsistencyError(f"Las columnas podrían no coincidir. Columnas de {table_name}: ({len(cols)}) {cols}, columnas del DataFrame o .csv: ({len(df_columns)}) {df_columns}")
    else:
        # Query para tumbar tabla si existe
        impala.run_sql_query(truncate_query, verbose = False)
        time.sleep(2)
        impala.run_sql_query(drop_query, verbose = False)
        time.sleep(2)

        # Query para crear la tabla
        impala.run_sql_query(create_query, verbose = False)
        time.sleep(2)

    # Query para poblar tabla vacía y contabilizar tiempo de ejecución
    start_time = time.time()
    impala.execute_many_batches(insert_query, df, batch_size)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\n\nTiempo tomado para llevar {df.shape[0]} registros a bodega fue: {elapsed_time/60:.2f} minutos")
    time.sleep(2)

    print(f"\nEjecutando queries COMPUTE, INVALIDATE y REFRESH sobre la tabla...")
    # Query para computar recursos
    impala.run_sql_query(compute_query, verbose = False)
    time.sleep(2)

    # Query para invalidar metadatos
    impala.run_sql_query(invalidate_query, verbose = False)
    time.sleep(2)

    # Query para refrescar tabla
    impala.run_sql_query(refresh_query, verbose = False)
    print("\nProceso terminado!")

# if __name__ == "__main__":
#     if len(sys.argv) != 5:
#         print("Debe pasar el nombre del archivo .csv, el separador (, o ;), el nombre de la tabla donde desea subir los datos y el tamaño del batch. \nEjemplo: subida_bodega.py subida_bodega.csv , work_sas.ada1jal_test 10000")
#     else:
#         csv_path = sys.argv[1]
#         table_name = sys.argv[2]
#         sep = sys.argv[3]
#         batch_size = sys.argv[4]
#         pipeline(csv_path, table_name, sep, batch_size)
#         print("\nProceso terminado!")