
import json
import os
import logging
from pathlib import Path

import pyodbc
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError


# =====================================================
# FILE PATHS
# =====================================================

BASE_DIR = Path(__file__).resolve().parent

SETTINGS_FILE = BASE_DIR / "settings.json"
CONFIG_FILE = BASE_DIR / "config.json"


# =====================================================
# LOGGING
# =====================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("busy_sync")


# =====================================================
# JSON FUNCTIONS
# =====================================================

def load_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        return json.load(file)


def save_config(config):
    """
    Save updated incremental values in config.json.
    """
    temp_file = CONFIG_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(config, file, indent=4, default=str)

    os.replace(temp_file, CONFIG_FILE)


# =====================================================
# SYNC FUNCTION
# =====================================================

def sync_job(cursor, mongo_db, settings, config, job):

    job_name = job["name"]
    collection_name = job["collection"]

    incremental_field = job["incremental_field"]
    unique_keys = job["unique_key"]

    # Support both a single key and composite keys.
    if isinstance(unique_keys, str):
        unique_keys = [unique_keys]

    if not unique_keys:
        raise ValueError(
            f"{job_name}: unique_key cannot be empty"
        )

    last_value = job.get("last_value", 0)

    collection = mongo_db[collection_name]

    logger.info(
        "Starting job: %s | Collection: %s | Last value: %s",
        job_name,
        collection_name,
        last_value
    )

    # -------------------------------------------------
    # FETCH SQL DATA
    # -------------------------------------------------

    cursor.execute(
        job["sql_query"],
        last_value
    )

    if cursor.description is None:
        raise ValueError(
            f"{job_name}: SQL query did not return a result set"
        )

    columns = [
        column[0]
        for column in cursor.description
    ]

    rows = cursor.fetchall()

    if not rows:
        logger.info(
            "%s: No new records found",
            job_name
        )
        return

    records = [
        dict(zip(columns, row))
        for row in rows
    ]

    operations = []
    max_value = last_value

    # -------------------------------------------------
    # PREPARE MONGODB DOCUMENTS
    # -------------------------------------------------

    for record in records:

        document = dict(record)

        incremental_value = document.get(incremental_field)

        if incremental_value is None:
            raise ValueError(
                f"{job_name}: Missing incremental field "
                f"'{incremental_field}'"
            )

        if incremental_value > max_value:
            max_value = incremental_value

        # Build the MongoDB unique filter.
        filter_doc = {
            "company_id": settings["company_id"]
        }

        for key in unique_keys:

            unique_value = document.get(key)

            if unique_value is None:
                raise ValueError(
                    f"{job_name}: Missing unique key '{key}'"
                )

            filter_doc[key] = unique_value

        document["company_id"] = settings["company_id"]

        # Insert new documents only.
        # Existing documents will not be overwritten.
        operations.append(
            UpdateOne(
                filter_doc,
                {
                    "$setOnInsert": document
                },
                upsert=True
            )
        )

    # -------------------------------------------------
    # INSERT INTO MONGODB
    # -------------------------------------------------

    result = collection.bulk_write(
        operations,
        ordered=False
    )

    logger.info(
        "%s: Fetched=%s | Inserted=%s | Existing=%s",
        job_name,
        len(records),
        result.upserted_count,
        len(records) - result.upserted_count
    )

    # -------------------------------------------------
    # UPDATE CHECKPOINT IN CONFIG.JSON
    # Only after successful MongoDB write
    # -------------------------------------------------

    job["last_value"] = max_value

    save_config(config)

    logger.info(
        "%s: Updated last_value to %s in config.json",
        job_name,
        max_value
    )


# =====================================================
# MAIN FUNCTION
# =====================================================

def main():

    settings = load_json(SETTINGS_FILE)
    config = load_json(CONFIG_FILE)

    mongo_uri = "mongodb+srv://ceoitbox:ceoitbox@slm.dlakl.mongodb.net/BharatForce?retryWrites=true&w=majority"

    if not mongo_uri:
        raise ValueError(
            "MONGODB_URI environment variable is not configured"
        )

    sql_conn = None
    mongo_client = None

    try:

        # -------------------------------------------------
        # SQL SERVER CONNECTION
        # -------------------------------------------------

        sql_conn = pyodbc.connect(
            settings["sqlconnection"],
            timeout=30
        )

        logger.info("BUSY SQL database connected")

        # -------------------------------------------------
        # MONGODB CONNECTION
        # -------------------------------------------------

        mongo_client = MongoClient(
            mongo_uri,
            serverSelectionTimeoutMS=10000
        )

        mongo_client.admin.command("ping")

        logger.info("MongoDB connected")

        mongo_db = mongo_client[settings["mongo_db"]]

        cursor = sql_conn.cursor()

        try:

            # -------------------------------------------------
            # PROCESS ALL CONFIGURED COLLECTIONS
            # -------------------------------------------------

            for job in config.get("jobs", []):

                try:
                    sync_job(
                        cursor,
                        mongo_db,
                        settings,
                        config,
                        job
                    )

                except Exception:
                    logger.exception(
                        "Sync failed for job: %s",
                        job.get("name", "unknown")
                    )

                    # Continue with the next collection.
                    continue

        finally:
            cursor.close()

    except Exception:
        logger.exception("Project execution failed")
        raise

    finally:

        if sql_conn is not None:
            sql_conn.close()

        if mongo_client is not None:
            mongo_client.close()

        logger.info("Connections closed")


# =====================================================
# ENTRY POINT
# =====================================================

if __name__ == "__main__":
    main()
