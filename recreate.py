#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from qdrant_client import QdrantClient, models

# === Параметры подключения (как в твоём коде) ===
QDRANT_HOST = "IP"
QDRANT_PORT = "PORT"
QDRANT_KEY  = (
    "API KEY"
)  # если нужен api_key, впиши сюда или возьми из переменной окружения
USE_HTTPS   = False

# === Настройки коллекции (как у тебя) ===
COLL = "kad_cases"
DIM  = 768
DIST = models.Distance.COSINE
PAYLOAD_INDEX_FIELDS = ("case_id", "court", "plaintiffs", "defendants")

def ensure_payload_indexes(qc: QdrantClient, collection: str):
    for field in PAYLOAD_INDEX_FIELDS:
        try:
            qc.create_payload_index(
                collection_name=collection,
                field_name=field,
                field_schema=models.PayloadSchemaType.KEYWORD,  # подходит и для строк, и для массива строк
            )
            print(f"✔ Создан payload-индекс по полю: {field}")
        except Exception as e:
            # Индекс уже существует — игнорируем
            print(f"… Индекс {field} возможно уже есть: {e}")

def drop_if_exists(qc: QdrantClient, collection: str):
    try:
        if qc.collection_exists(collection):
            print(f"⏳ Удаляю коллекцию: {collection}")
            qc.delete_collection(collection_name=collection)
            print(f"✔ Удалено: {collection}")
        else:
            print(f"ℹ Коллекции {collection} нет — удалять нечего")
    except Exception as e:
        print(f"⚠ Ошибка при удалении {collection}: {e}")
        raise

def create_collection(qc: QdrantClient, collection: str, dim: int, distance: models.Distance):
    print(f"⏳ Создаю коллекцию: {collection} (dim={dim}, distance={distance.value})")
    qc.create_collection(
        collection_name=collection,
        vectors_config=models.VectorParams(size=dim, distance=distance),
        # при необходимости можно явно задать HNSW/optimizers/quantization — у тебя они дефолтные
    )
    print(f"✔ Коллекция создана: {collection}")

def main():
    qc = QdrantClient(
        host=QDRANT_HOST,
        port=QDRANT_PORT,
        api_key=QDRANT_KEY,
        https=USE_HTTPS,
        timeout=30.0,
    )

    try:
        drop_if_exists(qc, COLL)
        create_collection(qc, COLL, DIM, DIST)
        ensure_payload_indexes(qc, COLL)
        # Быстрая проверка
        info = qc.get_collection(COLL)
        print(f"🎉 Готово. Статус коллекции: {info.status}")
    except Exception as e:
        print(f"💥 Не удалось пересоздать коллекцию {COLL}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
