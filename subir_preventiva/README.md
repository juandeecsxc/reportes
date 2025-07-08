# ETL: Carga de Excel a Impala SQL

Este proyecto permite cargar datos desde un archivo Excel a una tabla en Impala de forma automatizada y validada.

## 📁 Estructura
- `ejemplo_base_preventiva.xlsx`: Archivo Excel de ejemplo con el formato requerido.
- `main.py`: Punto de entrada para ejecutar el pipeline ETL.
- `.env`: Archivo de configuración de variables de entorno (ver `.env.example`).

## 🚀 Ejecución paso a paso

1. **Instala las dependencias**
   ```bash
   pip install -r requirements.txt
   ```

2. **Configura tus variables de entorno**
   - Copia `.env.example` a `.env` y edítalo con tus credenciales y rutas.
   - Asegúrate de que `EXCEL_PATH` apunte a `./ejemplo_base_preventiva.xlsx` o tu archivo real.

3. **Ejecuta el pipeline**
   ```bash
   python main.py
   ```
   El script leerá el Excel, validará las columnas y cargará los datos a la tabla SQL indicada.

## 📋 Formato requerido del Excel
El archivo debe tener las siguientes columnas:
- fuente
- Producto 2
- mes
- Agru. Ciclo
- Fecha Seguimiento
- Cuentas Norm
- Saldo Norm
- Cuentas Pendientes
- Saldo Pendiente
- Momentos
- Saldo Norm x Dia

Puedes usar `ejemplo_base_preventiva.xlsx` como plantilla.

## 🧑‍💻 Ejemplo de ejecución

```bash
python main.py
```

Salida esperada:
```
2024-05-01 12:00:00 INFO Iniciando pipeline ETL de Excel a SQL
2024-05-01 12:00:00 INFO Archivo Excel leído: ./ejemplo_base_preventiva.xlsx
2024-05-01 12:00:00 INFO Columnas validadas correctamente
2024-05-01 12:00:01 INFO Conexión a Impala exitosa
2024-05-01 12:00:01 INFO Tabla te_respaldamos.cob1jlu_base_preventiva truncada
2024-05-01 12:00:02 INFO Insertando registros 1-5 de 5
2024-05-01 12:00:02 INFO Datos insertados correctamente
2024-05-01 12:00:02 INFO Pipeline ETL finalizado
```

---

¿Dudas o mejoras? ¡Contribuye o abre un issue! 