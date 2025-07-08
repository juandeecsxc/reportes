# 1 activida  Prueba Técnica: ETL Python con Impala

## Objetivo final

Desarrollar un proceso ETL en Python capaz de leer datos desde un archivo Excel y cargarlos de forma segura y eficiente en una base de datos SQL Impala.

## 🎯 Objetivo

Diseñar e implementar un proceso ETL (Extract, Transform, Load) en Python que permita leer datos desde un archivo Excel y cargarlos en una base de datos Impala de manera eficiente y robusta.

## 📋 Insumos Disponibles

### Datos de Entrada
- **`subir_preve_excel.csv`**: Archivo con datos de ejemplo que contiene información de seguimiento de cuentas y saldos.

### Código Base
- **`subir_preventiva - copia.ipynb`**: Script base en Jupyter Notebook que contiene la lógica inicial pero presenta errores de conexión y lectura que deben ser corregidos.

## 📦 Entregables Esperados

El candidato debe entregar un proyecto modular con la siguiente estructura:

```
proyecto_etl/
├── config.py          # Configuración de conexiones y parámetros
├── etl.py            # Lógica principal del proceso ETL
├── main.py           # Punto de entrada de la aplicación
├── .env.example      # Plantilla de variables de entorno
├── requirements.txt  # Dependencias del proyecto
└── README.md         # Documentación del proyecto
```



 # Segunda Prueba Técnica – Integración SQL + Excel (LEFT JOIN y Exportación)

## 🎯 Objetivo

Automatizar el proceso de integración entre dos fuentes de datos:

* Una **tabla SQL existente** (`db_operaciones.operaciones`).
* Un **archivo Excel** (`subir_preve_excel.csv`) que debes cargar primero a SQL.

El candidato debe ejecutar una consulta SQL con un **LEFT JOIN por la columna `cedula`**, filtrar por rango de fechas (`fecha_operacion`) y exportar el resultado como un archivo Excel.

## 📂 Insumos entregados

1. `datos_extras.xlsx` (columnas: `cedula`, `fecha_extras`, etc.)
2. Acceso a la tabla SQL `db_operaciones.operaciones`.

## 🛠️ Requerimientos de la Prueba

1. **Carga del Excel a SQL**

   * Crear una tabla de entrada (ej.: `db_entrada.datos_extras`).
   * Puedes usar asistentes de carga SQL o un script en Python.

2. **Consulta de integración**

   * Realiza un `LEFT JOIN` entre `operaciones` y `datos_extras` usando `cedula`.
   * Aplica un filtro por rango de fechas: `fecha_operacion BETWEEN 'YYYY-MM-DD' AND 'YYYY-MM-DD'`.
   * Guarda el resultado en una tabla intermedia o final (ej.: `db_resultado.unificacion`).

3. **Exportación del resultado**

   * Genera un archivo Excel local con la tabla unificada.

## 🧩 Opciones de Implementación

### Opción A – SQL + Herramienta SQL

* Carga el Excel directamente desde el entorno SQL (Impala, Hue, DBeaver).
* Ejecuta la consulta con SQL y exporta el resultado manualmente.

### Opción B – 100% Python

* Lee `datos_extras.xlsx` con `pandas`.
* Conecta a SQL (p.ej., `pyodbc`) y carga el dataframe.
* Integra datos con `pandas.merge()` o ejecutando sentencias SQL.
* Exporta con `df.to_excel()`.

## ✅ Entregables

* **Código Python** (si aplica), limpio y comentado.
* **Script SQL** o consulta usada para la unión y el filtrado.
* **Archivo Excel** con el resultado final.
* Este **`README.md`** detallando:

  * Pasos realizados.
  * Descripción del `JOIN` y los filtros.
  * Herramientas y decisiones.


## 📌 Criterios de Evaluación

* Corrección del `LEFT JOIN` y el filtrado temporal.
* Éxito en la carga del Excel a SQL.
* Calidad, eficiencia y limpieza del código (SQL o Python).
* Manejo de errores y validación de datos.
* Claridad y profesionalismo en la documentación.
