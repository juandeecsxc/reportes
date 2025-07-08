import pandas as pd

def validate_columns(df: pd.DataFrame, required_columns: list):
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f'Faltan columnas requeridas: {missing}')
    return True

def validate_types(df: pd.DataFrame, column_types: dict):
    for col, dtype in column_types.items():
        if col in df.columns and not pd.api.types.is_dtype_equal(df[col].dtype, dtype):
            raise TypeError(f'Columna {col} no es del tipo {dtype}')
    return True 