"""Parte D: agregados por modelo y ventana en MongoDB.

1. Spark agrupa la muestra por modelo en las mismas ventanas que la Parte C (7 días, alineadas
   al primer día de datos) y calcula días-disco, fallas y AFR de cada par (modelo, ventana).
2. Los pares se guardan en MongoDB, un documento por par, y reemplazan los de la ejecución
   anterior.
3. Un pipeline de agregación de MongoDB devuelve, en cada ventana, los 5 modelos con mayor AFR
   que tengan un mínimo de días-disco, para no sacar conclusiones de muestras pequeñas.

Se calcula en batch sobre los CSV y con todos los modelos: el streaming de la Parte C publica
solo 5, y su verificación ya comprueba que sus ventanas coinciden con este mismo cálculo.
"""

import argparse
import datetime as dt
import os
from pathlib import Path

from pymongo import ASCENDING, MongoClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from drivestats.metrics import afr_pct
from part_a.mapreduce import group_by_header, load_dataframe
from part_c.events import window_offset_days
from part_c.producer import list_days
from part_c.streaming import collect_windows, span, window_column, windowed_counts

# Configuración de la conexión a MongoDB (en Compose: mongodb://mongo:27017/)
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = "drivestats_db"
COLLECTION_NAME = "model_aggregates"
DATA_INPUT_PATH = Path("data")
WINDOW_DAYS = 7  # el mismo tamaño que la Parte C; las ventanas no se solapan
TOP = 5


def to_utc(day: str) -> dt.datetime:
    """'AAAA-MM-DD' → fecha BSON (MongoDB guarda las fechas en UTC)."""
    return dt.datetime.fromisoformat(day).replace(tzinfo=dt.UTC)


def window_documents(rows: list[tuple[str, str, str, int, int]]) -> list[dict]:
    """(inicio, fin, modelo, días-disco, fallas) → un documento por modelo y ventana.

    La ventana es [window_start, window_end): window_end es el día siguiente al último.
    """
    return [
        {
            "model": model,
            "window_start": to_utc(start),
            "window_end": to_utc(end),
            "total_days": drive_days,
            "total_failures": failures,
            "afr": afr_pct(failures, drive_days),
        }
        for start, end, model, drive_days, failures in rows
    ]


def load_data_to_mongo(window_days: int = WINDOW_DAYS) -> int:
    """Calcula los agregados por modelo y ventana con Spark y los guarda en MongoDB."""
    # Sin CSV se detiene con error: nunca guarda datos de relleno como si fueran resultados
    days = list_days(DATA_INPUT_PATH)
    first = days[0][0]

    print("[Info] Procesando archivos de datos con Spark...")
    spark = (
        SparkSession.builder.appName("PartD-Standalone")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # La misma lectura y los mismos filtros que la Parte A (modelo no vacío, failure 0/1), así
    # los días-disco y las fallas coinciden con los suyos. Si falta una columna, se detiene.
    df = load_dataframe(spark, group_by_header(path for _, path in days)).withColumn(
        "event_time", F.to_timestamp("date", "yyyy-MM-dd")
    )
    # Las mismas ventanas y la misma agregación que el streaming de la Parte C
    window = window_column(window_days, window_days, window_offset_days(first, window_days))
    documents = window_documents(collect_windows(windowed_counts(df, window)))
    spark.stop()

    with MongoClient(MONGO_URI) as client:
        collection = client[DB_NAME][COLLECTION_NAME]
        # Limpiar datos anteriores; el índice único impide repetir un par (modelo, ventana)
        collection.delete_many({})
        collection.create_index([("window_start", ASCENDING), ("model", ASCENDING)], unique=True)
        if documents:
            collection.insert_many(documents)

    windows = len({d["window_start"] for d in documents})
    models = len({d["model"] for d in documents})
    print(
        f"[Éxito] Se guardaron {len(documents)} documentos en MongoDB: "
        f"{models} modelos en {windows} {'ventana' if windows == 1 else 'ventanas'} "
        f"de {window_days} días."
    )
    return len(documents)


def query_top_failing_models(min_disc_days=100):
    """Consulta en MongoDB, por ventana, los 5 modelos con mayor AFR y el umbral de días-disco."""
    with MongoClient(MONGO_URI) as client:
        collection = client[DB_NAME][COLLECTION_NAME]

        pipeline = [
            {"$match": {"total_days": {"$gte": min_disc_days}}},
            {
                "$group": {
                    "_id": {"start": "$window_start", "end": "$window_end"},
                    # $topN: los TOP primeros de cada ventana según sortBy (desempate por modelo)
                    "top": {
                        "$topN": {
                            "n": TOP,
                            "sortBy": {"afr": -1, "model": 1},
                            "output": {
                                "model": "$model",
                                "total_days": "$total_days",
                                "total_failures": "$total_failures",
                                "afr": "$afr",
                            },
                        }
                    },
                }
            },
            {"$sort": {"_id.start": 1}},
            {
                "$project": {
                    "_id": 0,
                    "window_start": {"$dateToString": {"date": "$_id.start", "format": "%Y-%m-%d"}},
                    "window_end": {"$dateToString": {"date": "$_id.end", "format": "%Y-%m-%d"}},
                    "top": 1,
                }
            },
        ]

        results = list(collection.aggregate(pipeline))

    print("\n" + "=" * 60)
    print(f" PARTE D: TOP {TOP} MODELOS CON MAYOR TASA DE FALLA (AFR) POR VENTANA")
    print(f" (mínimo {min_disc_days:,} días-disco en la ventana)")
    print("=" * 60)

    if not results:
        print("No se encontraron registros que superen el umbral mínimo.")

    for window in results:
        print(f"\nVentana {span(window['window_start'], window['window_end'])}\n")
        for i, doc in enumerate(window["top"], 1):
            print(f"{i}. Modelo: {doc.get('model')}")
            print(f"   Días-disco acumulados: {doc.get('total_days'):,}")
            print(f"   Fallas totales: {doc.get('total_failures')}")
            print(f"   Tasa de Falla Anualizada (AFR): {doc.get('afr'):.4f}%\n")

    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--window-days", type=int, default=WINDOW_DAYS, help="tamaño de la ventana (días)"
    )
    parser.add_argument(
        "--min-drive-days",
        type=int,
        default=100,
        help="días-disco mínimos en la ventana para entrar en el top",
    )
    args = parser.parse_args(argv)
    if args.window_days < 1:
        parser.error("--window-days debe ser al menos 1")
    return args


if __name__ == "__main__":
    args = parse_args()
    if load_data_to_mongo(args.window_days):
        query_top_failing_models(min_disc_days=args.min_drive_days)
