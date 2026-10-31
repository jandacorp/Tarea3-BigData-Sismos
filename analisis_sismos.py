#!/usr/bin/env python3
"""
Tarea 3 - Big Data
Analisis de patrones temporales, geograficos y de magnitud de la actividad sismica mundial.

Autor: Juan Pablo Ramirez Velasco
Fuente de datos: USGS - ANSS Comprehensive Earthquake Catalog (ComCat)

Uso recomendado:
    spark-submit analisis_sismos.py --descargar \
        --datos ./datos \
        --resultados ./resultados \
        --inicio 2000-01-01 \
        --fin 2025-12-31 \
        --min-mag 2.5

"""

import argparse
import csv
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    requests = None

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

USGS_QUERY = "https://earthquake.usgs.gov/fdsnws/event/1/query"
USGS_COUNT = "https://earthquake.usgs.gov/fdsnws/event/1/count"
MAX_EVENTOS_CONSULTA = 19000


def parse_args():
    parser = argparse.ArgumentParser(
        description="Procesamiento batch de eventos sismicos del catalogo USGS con Apache Spark."
    )
    parser.add_argument("--datos", default="./datos", help="Carpeta con los CSV de USGS")
    parser.add_argument("--resultados", default="./resultados", help="Carpeta de salida")
    parser.add_argument("--inicio", default="2000-01-01", help="Fecha inicial YYYY-MM-DD")
    parser.add_argument("--fin", default="2025-12-31", help="Fecha final YYYY-MM-DD")
    parser.add_argument("--min-mag", type=float, default=2.5, help="Magnitud minima")
    parser.add_argument(
        "--descargar",
        action="store_true",
        help="Descargar automaticamente los datos desde USGS antes del analisis",
    )
    return parser.parse_args()


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def contar_eventos(inicio, fin, min_mag):
    if requests is None:
        raise RuntimeError("Falta la libreria requests. Instale con: pip3 install requests")
    params = {
        "starttime": iso(inicio),
        "endtime": iso(fin),
        "minmagnitude": min_mag,
    }
    r = requests.get(USGS_COUNT, params=params, timeout=60)
    r.raise_for_status()
    return int(r.text.strip())


def intervalos_seguros(inicio, fin, min_mag):
    """Divide recursivamente el periodo para no superar el limite de eventos por consulta."""
    cantidad = contar_eventos(inicio, fin, min_mag)
    if cantidad <= MAX_EVENTOS_CONSULTA:
        return [(inicio, fin, cantidad)]

    duracion = fin - inicio
    if duracion <= timedelta(days=1):
        raise RuntimeError(
            f"El intervalo {inicio} - {fin} supera {MAX_EVENTOS_CONSULTA} eventos incluso en un dia."
        )

    mitad = inicio + duracion / 2
    return intervalos_seguros(inicio, mitad, min_mag) + intervalos_seguros(
        mitad, fin, min_mag
    )


def descargar_datos(carpeta, fecha_inicio, fecha_fin, min_mag):
    if requests is None:
        raise RuntimeError("Falta la libreria requests. Instale con: pip3 install requests")

    carpeta = Path(carpeta)
    carpeta.mkdir(parents=True, exist_ok=True)

    inicio = datetime.fromisoformat(fecha_inicio).replace(tzinfo=timezone.utc)
    # Se suma un dia para incluir completamente la fecha final.
    fin = datetime.fromisoformat(fecha_fin).replace(tzinfo=timezone.utc) + timedelta(days=1)

    print("\n[1/6] Calculando intervalos de descarga seguros...")
    intervalos = intervalos_seguros(inicio, fin, min_mag)
    total_estimado = sum(x[2] for x in intervalos)
    print(f"Intervalos: {len(intervalos)} | Eventos estimados: {total_estimado:,}")

    manifest = carpeta / "manifest_descarga.csv"
    with manifest.open("w", newline="", encoding="utf-8") as mf:
        writer = csv.writer(mf)
        writer.writerow(["archivo", "inicio", "fin", "eventos_estimados", "estado"])

        for idx, (ini, ffin, cantidad) in enumerate(intervalos, start=1):
            nombre = f"sismos_{idx:04d}_{ini.strftime('%Y%m%d')}_{ffin.strftime('%Y%m%d')}.csv"
            destino = carpeta / nombre

            if destino.exists() and destino.stat().st_size > 100:
                print(f"[{idx}/{len(intervalos)}] Ya existe: {nombre}")
                writer.writerow([nombre, iso(ini), iso(ffin), cantidad, "existente"])
                continue

            params = {
                "format": "csv",
                "starttime": iso(ini),
                "endtime": iso(ffin),
                "minmagnitude": min_mag,
                "orderby": "time-asc",
            }
            print(
                f"[{idx}/{len(intervalos)}] Descargando {cantidad:,} eventos: "
                f"{ini.date()} -> {ffin.date()}"
            )
            r = requests.get(USGS_QUERY, params=params, timeout=120)
            r.raise_for_status()
            destino.write_bytes(r.content)
            writer.writerow([nombre, iso(ini), iso(ffin), cantidad, "descargado"])
            time.sleep(0.15)

    print(f"Descarga terminada. Manifest: {manifest}")


def guardar_csv(df, ruta):
    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(str(ruta))
    )


def generar_graficas(resultados, por_anio, por_magnitud, por_profundidad):
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        print("[AVISO] matplotlib no esta instalado. Se omiten las graficas PNG.")
        print("        Para instalarlas: pip3 install matplotlib")
        return

    resultados = Path(resultados)
    resultados.mkdir(parents=True, exist_ok=True)

    filas = por_anio.orderBy("anio").collect()
    if filas:
        plt.figure(figsize=(10, 5))
        plt.plot([r["anio"] for r in filas], [r["eventos"] for r in filas], marker="o")
        plt.title("Eventos sismicos por ano")
        plt.xlabel("Ano")
        plt.ylabel("Numero de eventos")
        plt.grid(True, alpha=0.25)
        plt.tight_layout()
        plt.savefig(resultados / "eventos_por_anio.png", dpi=160)
        plt.close()

    filas = por_magnitud.orderBy("orden").collect()
    if filas:
        plt.figure(figsize=(9, 5))
        plt.bar([r["rango_magnitud"] for r in filas], [r["eventos"] for r in filas])
        plt.title("Distribucion de eventos por magnitud")
        plt.xlabel("Rango de magnitud")
        plt.ylabel("Numero de eventos")
        plt.xticks(rotation=30, ha="right")
        plt.tight_layout()
        plt.savefig(resultados / "eventos_por_magnitud.png", dpi=160)
        plt.close()

    filas = por_profundidad.orderBy("orden").collect()
    if filas:
        plt.figure(figsize=(8, 5))
        plt.bar([r["rango_profundidad"] for r in filas], [r["eventos"] for r in filas])
        plt.title("Distribucion de eventos por profundidad")
        plt.xlabel("Rango de profundidad")
        plt.ylabel("Numero de eventos")
        plt.xticks(rotation=20, ha="right")
        plt.tight_layout()
        plt.savefig(resultados / "eventos_por_profundidad.png", dpi=160)
        plt.close()

    print(f"Graficas generadas en: {resultados.resolve()}")


def main():
    args = parse_args()

    if args.descargar:
        descargar_datos(args.datos, args.inicio, args.fin, args.min_mag)

    carpeta_datos = Path(args.datos)
    archivos = list(carpeta_datos.glob("*.csv"))
    archivos = [x for x in archivos if x.name != "manifest_descarga.csv"]
    if not archivos:
        print(f"No se encontraron CSV en {carpeta_datos.resolve()}")
        print("Ejecute nuevamente con --descargar o copie los CSV del USGS a esa carpeta.")
        sys.exit(1)

    resultados = Path(args.resultados)
    resultados.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("Tarea3_Analisis_Sismos_USGS")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("\n[2/6] Cargando CSV con Spark...")
    df_raw = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .option("mode", "PERMISSIVE")
        .csv([str(x) for x in archivos])
    )

    total_raw = df_raw.count()
    print(f"Registros cargados: {total_raw:,}")
    print("Esquema original:")
    df_raw.printSchema()

    print("\n[3/6] Limpieza y transformacion...")
    columnas_requeridas = ["time", "latitude", "longitude", "depth", "mag", "place", "id"]
    faltantes = [c for c in columnas_requeridas if c not in df_raw.columns]
    if faltantes:
        raise RuntimeError(f"Faltan columnas esperadas en los CSV de USGS: {faltantes}")

    df = (
        df_raw
        .withColumn("fecha_hora", F.to_timestamp("time"))
        .withColumn("magnitud", F.col("mag").cast(DoubleType()))
        .withColumn("profundidad_km", F.col("depth").cast(DoubleType()))
        .withColumn("latitud", F.col("latitude").cast(DoubleType()))
        .withColumn("longitud", F.col("longitude").cast(DoubleType()))
        .dropDuplicates(["id"])
        .filter(F.col("fecha_hora").isNotNull())
        .filter(F.col("magnitud").isNotNull())
        .filter(F.col("profundidad_km").isNotNull())
        .filter(F.col("latitud").isNotNull() & F.col("longitud").isNotNull())
    )

    if "type" in df.columns:
        df = df.filter((F.col("type") == "earthquake") | F.col("type").isNull())

    df = (
        df.withColumn("anio", F.year("fecha_hora"))
        .withColumn("mes", F.month("fecha_hora"))
        .withColumn(
            "rango_magnitud",
            F.when(F.col("magnitud") < 3.0, "2.5-2.9")
            .when(F.col("magnitud") < 4.0, "3.0-3.9")
            .when(F.col("magnitud") < 5.0, "4.0-4.9")
            .when(F.col("magnitud") < 6.0, "5.0-5.9")
            .when(F.col("magnitud") < 7.0, "6.0-6.9")
            .otherwise("7.0 o mas"),
        )
        .withColumn(
            "orden_magnitud",
            F.when(F.col("magnitud") < 3.0, 1)
            .when(F.col("magnitud") < 4.0, 2)
            .when(F.col("magnitud") < 5.0, 3)
            .when(F.col("magnitud") < 6.0, 4)
            .when(F.col("magnitud") < 7.0, 5)
            .otherwise(6),
        )
        .withColumn(
            "rango_profundidad",
            F.when(F.col("profundidad_km") < 70, "Superficial (<70 km)")
            .when(F.col("profundidad_km") <= 300, "Intermedia (70-300 km)")
            .otherwise("Profunda (>300 km)"),
        )
        .withColumn(
            "orden_profundidad",
            F.when(F.col("profundidad_km") < 70, 1)
            .when(F.col("profundidad_km") <= 300, 2)
            .otherwise(3),
        )
        .withColumn(
            "region_aprox",
            F.when(
                F.col("place").contains(","),
                F.trim(F.regexp_extract(F.col("place"), r",\s*(.+)$", 1)),
            ).otherwise(F.col("place")),
        )
    )

    total_limpio = df.count()
    print(f"Registros despues de limpieza: {total_limpio:,}")
    print(f"Registros descartados/duplicados: {total_raw - total_limpio:,}")

    print("\n[4/6] Analisis exploratorio...")
    resumen = df.agg(
        F.count("*").alias("total_eventos"),
        F.round(F.min("magnitud"), 2).alias("magnitud_minima"),
        F.round(F.max("magnitud"), 2).alias("magnitud_maxima"),
        F.round(F.avg("magnitud"), 3).alias("magnitud_promedio"),
        F.round(F.avg("profundidad_km"), 2).alias("profundidad_promedio_km"),
        F.round(F.max("profundidad_km"), 2).alias("profundidad_maxima_km"),
    )
    resumen.show(truncate=False)

    por_anio = df.groupBy("anio").agg(F.count("*").alias("eventos")).orderBy("anio")
    por_mes = (
        df.groupBy("anio", "mes")
        .agg(F.count("*").alias("eventos"), F.round(F.avg("magnitud"), 3).alias("magnitud_promedio"))
        .orderBy("anio", "mes")
    )
    por_magnitud = (
        df.groupBy("rango_magnitud", "orden_magnitud")
        .agg(F.count("*").alias("eventos"))
        .withColumnRenamed("orden_magnitud", "orden")
        .orderBy("orden")
    )
    por_profundidad = (
        df.groupBy("rango_profundidad", "orden_profundidad")
        .agg(F.count("*").alias("eventos"))
        .withColumnRenamed("orden_profundidad", "orden")
        .orderBy("orden")
    )
    top_regiones = (
        df.filter(F.col("region_aprox").isNotNull() & (F.length("region_aprox") > 0))
        .groupBy("region_aprox")
        .agg(F.count("*").alias("eventos"), F.round(F.avg("magnitud"), 3).alias("magnitud_promedio"))
        .orderBy(F.desc("eventos"))
        .limit(20)
    )
    mas_fuertes = (
        df.select("fecha_hora", "magnitud", "profundidad_km", "place", "latitud", "longitud", "id")
        .orderBy(F.desc("magnitud"), F.desc("fecha_hora"))
        .limit(20)
    )

    print("\nEventos por ano:")
    por_anio.show(30, truncate=False)
    print("\nDistribucion por magnitud:")
    por_magnitud.show(truncate=False)
    print("\nDistribucion por profundidad:")
    por_profundidad.show(truncate=False)
    print("\n20 regiones aproximadas con mayor cantidad de eventos:")
    top_regiones.show(20, truncate=False)
    print("\n20 eventos de mayor magnitud:")
    mas_fuertes.show(20, truncate=False)

    print("\n[5/6] Guardando resultados...")
    guardar_csv(resumen, resultados / "resumen_general")
    guardar_csv(por_anio, resultados / "eventos_por_anio")
    guardar_csv(por_mes, resultados / "eventos_por_mes")
    guardar_csv(por_magnitud, resultados / "eventos_por_magnitud")
    guardar_csv(por_profundidad, resultados / "eventos_por_profundidad")
    guardar_csv(top_regiones, resultados / "top_regiones")
    guardar_csv(mas_fuertes, resultados / "eventos_mayor_magnitud")

    print("\n[6/6] Generando visualizaciones...")
    generar_graficas(resultados, por_anio, por_magnitud, por_profundidad)

    print("\nProceso finalizado correctamente.")
    print(f"Resultados: {resultados.resolve()}")
    spark.stop()


if __name__ == "__main__":
    main()
