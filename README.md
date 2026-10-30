# Tarea 3 - Big Data: analisis de actividad sismica con Apache Spark

**Autor:** Juan Pablo Ramirez Velasco  
**Curso:** Big Data - UNAD  
**Actividad:** Tarea 3 - Procesamiento de Datos con Apache Spark

## 1. Problema

El proyecto analiza patrones temporales, geograficos y de magnitud de la actividad sismica mundial mediante Apache Spark. El objetivo es procesar un conjunto amplio de eventos sismicos para identificar tendencias por fecha, magnitud, profundidad y ubicacion.

## 2. Fuente de datos

Los datos provienen del **ANSS Comprehensive Earthquake Catalog (ComCat)** del U.S. Geological Survey (USGS).

- Catalogo: https://earthquake.usgs.gov/data/comcat/
- Buscador: https://earthquake.usgs.gov/earthquakes/search/
- Servicio FDSN Event: https://earthquake.usgs.gov/fdsnws/event/1/

El script permite descargar automaticamente eventos para el periodo 2000-2025 con magnitud minima 2.5. Para evitar el limite de resultados por consulta, el periodo se divide automaticamente en intervalos seguros.

## 3. Requisitos

- Python 3
- Apache Spark / PySpark
- Java compatible con la version de Spark instalada
- Libreria `requests` para la descarga desde USGS
- Libreria `matplotlib` (opcional, para generar graficas PNG)

Instalacion de librerias Python:

```bash
pip3 install requests matplotlib
```

## 4. Estructura sugerida

```text
Tarea3_Sismos/
├── analisis_sismos.py
├── README.md
├── datos/
└── resultados/
```

## 5. Ejecucion completa

Desde la carpeta del proyecto:

```bash
spark-submit analisis_sismos.py \
  --descargar \
  --datos ./datos \
  --resultados ./resultados \
  --inicio 2000-01-01 \
  --fin 2025-12-31 \
  --min-mag 2.5
```

El parametro `--descargar` obtiene los CSV directamente del USGS antes de iniciar el procesamiento.

Si los CSV ya estan almacenados en `./datos`, puede ejecutar:

```bash
spark-submit analisis_sismos.py \
  --datos ./datos \
  --resultados ./resultados
```

## 6. Procesamiento realizado

El programa realiza las siguientes tareas con PySpark y DataFrames:

1. Carga de todos los CSV disponibles en la carpeta de datos.
2. Inferencia del esquema y validacion de columnas principales.
3. Eliminacion de registros duplicados mediante el identificador del evento.
4. Tratamiento de valores nulos en fecha, magnitud, profundidad y coordenadas.
5. Conversion de fechas y variables numericas.
6. Creacion de variables derivadas: ano, mes, rango de magnitud y rango de profundidad.
7. Calculo de estadisticas generales de magnitud y profundidad.
8. Conteo de eventos por ano y mes.
9. Distribucion de eventos por rangos de magnitud.
10. Distribucion por profundidad.
11. Identificacion de regiones aproximadas con mayor numero de eventos.
12. Identificacion de los eventos de mayor magnitud.
13. Exportacion de resultados en CSV.
14. Generacion opcional de graficas PNG.

## 7. Resultados esperados

La carpeta `resultados` contiene directorios CSV generados por Spark y, si `matplotlib` esta instalado, las siguientes graficas:

- `eventos_por_anio.png`
- `eventos_por_magnitud.png`
- `eventos_por_profundidad.png`

Tambien se generan resultados tabulares para:

- resumen general;
- eventos por ano;
- eventos por mes;
- distribucion por magnitud;
- distribucion por profundidad;
- regiones con mayor cantidad de eventos;
- eventos de mayor magnitud.

## 8. Evidencias para la entrega

Durante la ejecucion se recomienda capturar:

- version de Spark y Java;
- carga del dataset;
- esquema del DataFrame;
- cantidad de registros antes y despues de la limpieza;
- resumen estadistico;
- eventos por ano;
- distribucion por magnitud y profundidad;
- eventos de mayor magnitud;
- graficas generadas;
- ejecucion completa mediante `spark-submit`.

## 9. Componente Kafka / Spark Streaming

El procesamiento en tiempo real se desarrolla por separado siguiendo el **Anexo 3 - Instructivo Spark Streaming y Kafka** suministrado por el curso. Este repositorio puede incorporar posteriormente los scripts del productor Kafka y del consumidor Spark Streaming.
