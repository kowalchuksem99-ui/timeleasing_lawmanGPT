import pathlib
import uuid
import re
import time
import tqdm
import os

import tiktoken
from openai import OpenAI, OpenAIError          # ← тип ошибки пригодится
from qdrant_client import QdrantClient, models
from typing import Optional, List

# ––– Parameters ––––––––––––––––––––––––––––––––––––––––
OPENAI_KEY  = (
    "API KEY"
)
QDRANT_KEY  = OPENAI_KEY          # можно задать другой ключ
QDRANT_HOST = "IP"
QDRANT_PORT = "PORT"

# === Периодический индексатор (STEP_THREE) =========================
INDEX_POLL_SEC   = 120   # базовый интервал опроса, сек
INDEX_MAX_BACKOFF = 900  # максимум бэкоффа, сек (15 минут)
FILE_STABLE_SEC   = 2    # файл считаем «готовым», если не менялся >= N сек

SRC_DIR   = r"C:\Users\User\Desktop\text_txt"
COLL      = "kad_cases"
EMB_MODEL = "MODEL"
DIM       = 768

CHUNK     = 800      # размер блока в токенах
OVERLAP   = 160      # перекрытие
BATCH     = 128      # сколько точек отправлять за раз

# Суффикс, который будет вставлен ПЕРЕД расширением, например
#   «decision.txt» → «decision.indexed.txt»
PROCESSED_TAG = ".indexed"

CASE_RE = re.compile(
    r"(?:[АA]\d{1,3}-\d{1,7}/\d{4}|СИП-\d{1,7}(?:[-/]\d{4})?)",
    re.IGNORECASE
)
NUM_MAP = {
    "0": "НОЛЬ", "1": "ОДИН", "2": "ДВА", "3": "ТРИ", "4": "ЧЕТЫРЕ",
    "5": "ПЯТЬ", "6": "ШЕСТЬ", "7": "СЕМЬ", "8": "ВОСЕМЬ", "9": "ДЕВЯТЬ",
    "-": "ТИРЕ", "/": "СЛЕШ", "A": "А", "B": "Б",
}

to_words = lambda s: " ".join(NUM_MAP.get(ch.upper(), ch) for ch in s)

# ––– Новое: исключение и детектор ошибок бюджета –––––––
class InsufficientFundsError(RuntimeError):
    """Прокидывается наверх, если у аккаунта закончились средства."""

def is_insufficient_funds(exc: Exception) -> bool:     # NEW
    """
    Очень разные версии openai-python формируют тексты/коды ошибок
    по-разному, поэтому смотрим и на code, и на текст.
    """
    text = str(exc).lower()
    return any(tok in text for tok in ("insufficient", "quota", "balance"))

# ––– Clients –––––––––––––––––––––––––––––––––––––––––––
enc     = tiktoken.encoding_for_model(EMB_MODEL)
openai  = OpenAI(api_key=OPENAI_KEY)
qdrant  = QdrantClient(
    host=QDRANT_HOST,
    port=QDRANT_PORT,
    api_key=QDRANT_KEY,
    https=False,
    timeout=30.0,
)

# --- Шапка дела: Суд / Истец / Ответчик / Номер дела -------------------------

HEADER_SLICE = 6000  # как было

def _grab(label: str, head: str) -> Optional[str]:
    """
    Извлекает значение после одного из ярлыков (label)
    до следующего ярлыка или =====. Устойчиво к CRLF.
    """
    if not head:
        return None
    # Нормализуем переносы строк: CRLF/CR -> LF
    head = head.replace("\r\n", "\n").replace("\r", "\n")

    # ВАЖНО: группируем альтернацию ярлыка (?:{label})
    pat = rf"(?im)^\s*(?:{label})\s*:\s*(.+?)\s*(?=\n\s*(?:Номер\s*дела|Суд|Истец|Ответчик)\s*:|\n\s*={3,}|$)"
    m = re.search(pat, head)
    if not m:
        return None
    val = m.group(1)
    return val.strip() if isinstance(val, str) else None

def _clean_name(s: str) -> str:
    s = s.replace("\u00A0", " ")            # неразрывные пробелы
    s = re.sub(r"[«»\"'“”]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _split_many(s: Optional[str]) -> List[str]:
    """Безопасно разбивает список сторон на элементы."""
    if not s:
        return []
    parts = re.split(r"\s*(?:;|,|\s+\bи\b\s*)\s*", s, flags=re.IGNORECASE)
    return [_clean_name(p) for p in parts if p and p.strip()]

CASE_IN_TEXT_RE = re.compile(
    r"(?im)^\s*Номер\s*дела\s*:\s*([АA]\d{1,3}-\d{1,7}/\d{4}|СИП-\d{1,7}(?:[-/]\d{4})?)"
)

def parse_header_fields(text: str) -> dict:
    """
    Возвращает: {'case_id', 'court', 'plaintiffs', 'defendants'}
    """
    if not text:
        return {"case_id": None, "court": None, "plaintiffs": [], "defendants": []}

    # Нормализуем переносы один раз
    head = text[:HEADER_SLICE].replace("\r\n", "\n").replace("\r", "\n")

    m_case = CASE_IN_TEXT_RE.search(head)
    case_in_text = m_case.group(1).upper() if m_case else None

    court_raw = _grab(r"Суд", head)
    istec_raw = _grab(r"Истец|Заявитель|Административн\w*\s+истец", head)
    otv_raw   = _grab(r"Ответчик|Административн\w*\s+ответчик|Заинтересован\w*", head)

    court = _clean_name(court_raw) if court_raw else None
    plaintiffs = _split_many(istec_raw)
    defendants = _split_many(otv_raw)

    return {
        "case_id": case_in_text,
        "court": court,
        "plaintiffs": plaintiffs,
        "defendants": defendants,
    }


# ––– Helper functions ––––––––––––––––––––––––––––––––––

def chunker(text: str):
    """Yield overlapping chunks of text, each ≈CHUNK tokens."""
    tokens = enc.encode(text)
    step   = CHUNK - OVERLAP
    for i in range(0, len(tokens), step):
        yield enc.decode(tokens[i : i + CHUNK])


def extract_case(filename: str) -> str:
    m = CASE_RE.search(filename)
    return m.group(0).upper() if m else "UNKNOWN"


def mark_processed(path: pathlib.Path) -> pathlib.Path:
    """Return new Path with PROCESSED_TAG inserted **before** extension."""
    if path.suffix:  # «file.txt» → «file.indexed.txt»
        return path.with_name(f"{path.stem}{PROCESSED_TAG}{path.suffix}")
    # у файла нет расширения
    return path.with_name(path.name + PROCESSED_TAG)


def _file_is_stable(path: pathlib.Path, stable_sec: int = FILE_STABLE_SEC) -> bool:
    """Файл считаем готовым, если его mtime старше stable_sec."""
    try:
        mtime = os.path.getmtime(str(path))
        return (time.time() - mtime) >= stable_sec
    except FileNotFoundError:
        return False


def flush_batches(buf, *, wait=False):
    if buf:
        try:
            qdrant.upsert(COLL, points=buf, wait=wait)
        except Exception as exc:
            print(f"⚠ Qdrant upsert failed (batch size {len(buf)}): {exc}")
        finally:
            buf.clear()


def ensure_payload_indexes():
    for field in ("case_id", "court", "plaintiffs", "defendants"):
        try:
            qdrant.create_payload_index(
                collection_name=COLL,
                field_name=field,
                field_schema=models.PayloadSchemaType.KEYWORD,
            )
        except Exception:
            pass  # уже есть или создастся позже

def ensure_collection():
    try:
        qdrant.get_collection(COLL)
    except Exception:
        print("⏳ Создаю коллекцию…")
        qdrant.create_collection(
            collection_name=COLL,
            vectors_config=models.VectorParams(size=DIM, distance=models.Distance.COSINE),
        )
    # ← гарантируем индексы
    ensure_payload_indexes()


# ––– Main indexing routine ––––––––––––––––––––––––––––

def index_all() -> int:
    """Индексирует все НЕ обработанные TXT из SRC_DIR. Возвращает кол-во новых файлов."""
    ensure_collection()
    points_buf = []
    processed_files = 0

    for path in tqdm.tqdm(pathlib.Path(SRC_DIR).glob("*.txt"), desc="Файлы"):
        # Уже помечен как .indexed — пропускаем
        if path.name.endswith(PROCESSED_TAG + path.suffix):
            continue
        # Пропускаем «недописанные» файлы
        if not _file_is_stable(path, FILE_STABLE_SEC):
            continue

        filename = path.name
        case_num = extract_case(filename)
        raw_text = path.read_text(encoding="utf-8")

        info = parse_header_fields(raw_text)
        if case_num == "UNKNOWN" and info.get("case_id"):
            case_num = info["case_id"]

        court = info["court"]
        plaintiffs = info["plaintiffs"]
        defendants = info["defendants"]

        index_tag = f"<CASE:{case_num}>"
        if court:
            index_tag += f" <COURT:{court}>"
        if plaintiffs:
            index_tag += f" <ISTEC:{';'.join(plaintiffs[:2])}>"
        if defendants:
            index_tag += f" <OTV:{';'.join(defendants[:2])}>"
        index_tag += "\n"

        for chunk in chunker(raw_text):
            text_block = index_tag + chunk
            try:
                vec = openai.embeddings.create(
                    model=EMB_MODEL, input=text_block, dimensions=DIM
                ).data[0].embedding
            except OpenAIError as exc:
                if is_insufficient_funds(exc):
                    print("💸 Недостаточно средств/квоты OpenAI — останавливаю индексацию.")
                    raise InsufficientFundsError from exc
                print(f"⚠ Embedding failed ({filename}): {exc}. Чанк пропущен.")
                continue
            except Exception as exc:
                print(f"⚠ Embedding failed ({filename}): {exc}. Чанк пропущен.")
                continue

            points_buf.append(
                models.PointStruct(
                    id=str(uuid.uuid4()),
                    vector=vec,
                    payload={
                        "file": filename,
                        "text": text_block,
                        "case_id": case_num,
                        "court": court,
                        "plaintiffs": plaintiffs,
                        "defendants": defendants,
                    },
                )
            )

            if len(points_buf) == BATCH:
                flush_batches(points_buf)

        # «хвост» по файлу и пометка как обработанный
        flush_batches(points_buf, wait=True)
        points_buf.clear()

        new_path = mark_processed(path)
        try:
            path.rename(new_path)
            processed_files += 1
            print(f"✔ Обработан: {new_path.name}")
        except Exception as exc:
            print(f"⚠ Не удалось переименовать {filename}: {exc}")

    if processed_files:
        print(f"🎉 Индексация завершена: новых файлов — {processed_files}")
    else:
        print("ℹ Новых файлов для индексации не найдено")
    return processed_files


# ––– Auto-restart wrapper ––––––––––––––––––––––––––––––

def STEP_THREE(poll_sec: int = INDEX_POLL_SEC, max_backoff: int = INDEX_MAX_BACKOFF):
    """
    Демон: периодически смотрит в SRC_DIR, индексирует новые файлы.
    Если новых файлов нет — увеличивает паузу (экспоненциальный бэкофф) до max_backoff.
    При появлении новых — пауза сбрасывается к poll_sec.
    """
    retries = 0
    backoff = poll_sec
    while True:
        try:
            n = index_all()
            retries = 0
            if n == 0:
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)
            else:
                backoff = poll_sec
                time.sleep(poll_sec)

        except InsufficientFundsError:
            print("⏹ Индексатор остановлен: нулевой баланс/квота OpenAI.")
            break

        except KeyboardInterrupt:
            print("⏹ Индексатор: останов по Ctrl+C.")
            break

        except Exception as exc:
            wait = min(60, 2 ** retries)
            print(f"💥 STEP_THREE fatal: {exc!r}. Перезапуск через {wait} сек…")
            time.sleep(wait)
            retries += 1

STEP_THREE()