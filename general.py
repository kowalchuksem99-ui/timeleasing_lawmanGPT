"""
Пак документов - набор документов из одного арбитражного дела
"""
import threading
import time
import logging
import os
import re
import shutil
import json
from contextlib import suppress
from path import Path

from stepone_parser import STEP_ONE, MANIFEST_EXT, _safe_case_for_prefix  # + импортируем manifest helpers
from steptwo_handler import STEP_TWO
from stepthree_index import STEP_THREE

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

PDF_SOURCE_DIR = Path(r"C:\Users\User\Desktop\test_pdf")
PDF_DESTINATION_DIR = Path(r"C:\Users\User\Desktop\test_txt")
LIMBO_SOURCE_DIR = Path(r"C:\Users\User\Desktop\sorted_pdf")
LIMBO_DIR = Path(str(LIMBO_SOURCE_DIR))   # у тебя это sorted_pdf
MANIFEST_GLOB = "*.manifest.json"

# === Периодический индексатор (STEP_THREE) =========================
INDEX_POLL_SEC    = 120   # базовый интервал опроса, сек
INDEX_MAX_BACKOFF = 900   # максимум бэкоффа, сек (15 минут)
FILE_STABLE_SEC   = 2     # файл считаем «готовым», если не менялся >= N сек


CASE_RE = re.compile(r"[AА]\d{1,3}[-–—]\d{1,7}[_/]\d{4}", re.IGNORECASE)

def relocate_pack(pdf_dir: Path, limbo_dir: Path, parse_thread: threading.Thread):
    pdf_dir = str(pdf_dir)
    limbo_dir = str(limbo_dir)
    os.makedirs(limbo_dir, exist_ok=True)

    try:
        while True:
            # 1) готовые манифесты
            manifests = [f for f in os.listdir(pdf_dir) if f.endswith(MANIFEST_EXT)]
            ready_cases = []

            for mf in manifests:
                try:
                    with open(os.path.join(pdf_dir, mf), "r", encoding="utf-8") as f:
                        man = json.load(f)
                except Exception:
                    continue

                if man.get("status") != "complete":
                    continue

                case_no = man.get("case_no") or ""
                safe_case = man.get("safe_case") or _safe_case_for_prefix(case_no)
                prefix = f"{safe_case} — "

                # если внезапно остались .crdownload по этому делу — не трогаем
                has_partial = any(
                    fn.lower().endswith(".crdownload") and fn.startswith(safe_case)
                    for fn in os.listdir(pdf_dir)
                )
                if has_partial:
                    continue

                pack_files = [fn for fn in os.listdir(pdf_dir)
                              if fn.startswith(prefix) and fn.lower().endswith(".pdf")]

                expected = int(man.get("expected") or 0)
                have = int(man.get("have") or 0)
                if expected and len(pack_files) < min(expected, have):
                    continue

                ready_cases.append((case_no, safe_case, pack_files, mf))

            if not ready_cases:
                if not parse_thread.is_alive():
                    # парсер умер — если нет PDF, выходим
                    if not any(f.lower().endswith(".pdf") for f in os.listdir(pdf_dir)):
                        break
                time.sleep(2)
                continue

            # 2) переносим готовые паки
            for case_no, safe_case, pack_files, manifest_file in ready_cases:
                for fn in pack_files:
                    src = os.path.join(pdf_dir, fn)
                    dst = os.path.join(limbo_dir, fn)
                    try:
                        shutil.move(src, dst)
                        logging.info(f"[Relocate] {fn} → LIMBO ({safe_case})")
                    except FileNotFoundError:
                        pass
                    except Exception as e:
                        logging.warning(f"[Relocate] Не удалось переместить {fn}: {e}")

                # манифест следом
                try:
                    shutil.move(os.path.join(pdf_dir, manifest_file),
                                os.path.join(limbo_dir, manifest_file))
                except Exception:
                    with suppress(Exception):
                        os.remove(os.path.join(pdf_dir, manifest_file))

            time.sleep(1)

    except Exception as err:
        logging.exception(f"Error in 'relocate_pack': {err}")


def handler_loop_reactive(parse_thread: threading.Thread, poll_sec: int = 2):
    """Ждём манифесты в LIMBO (sorted_pdf) и запускаем STEP_TWO(), когда пришёл новый пак."""
    # вместо LIMBO_DIR.mkdir(...)
    os.makedirs(str(LIMBO_DIR), exist_ok=True)
    seen = set()

    while True:
        manifests = list(LIMBO_DIR.glob(MANIFEST_GLOB))
        processed = False

        for mf in manifests:
            try:
                man = json.loads(mf.open(encoding="utf-8").read())
            except Exception:
                continue

            if man.get("status") != "complete":
                continue

            key = mf.name
            if key in seen:
                continue

            processed = True
            seen.add(key)

        if processed:
            try:
                STEP_TWO()
            except Exception as e:
                logging.exception(f"[Handler] STEP_TWO error: {e}")

            # удаляем только те манифесты, что обработали (а не все подряд)
            for name in list(seen):
                mf_path = LIMBO_DIR / name
                try:
                    mf_path.remove()  # у path.Path это метод .remove()
                    seen.discard(name)
                except Exception:
                    pass

        if not parse_thread.is_alive() and not list(LIMBO_DIR.glob(MANIFEST_GLOB)):
            time.sleep(1)
            if not list(LIMBO_DIR.glob(MANIFEST_GLOB)):
                logging.info("[Handler] Парсер завершён, манифестов нет — выходим из обработчика.")
                break

        time.sleep(poll_sec)

# Первый шаг - скачивание файлов
thread_1 = threading.Thread(target=STEP_ONE, name="Parser")

# Второй поток - релокация, как daemon
thread_2 = threading.Thread(
    target=relocate_pack,
    args=(PDF_SOURCE_DIR, LIMBO_SOURCE_DIR, thread_1),
    name="Relocate",
    daemon=True
)

thread_3 = threading.Thread(
    target=handler_loop_reactive,
    args=(thread_1, 2),
    name="Handler",
    daemon=True
)

thread_4 = threading.Thread(
    target=STEP_THREE,
    kwargs={"poll_sec": INDEX_POLL_SEC, "max_backoff": INDEX_MAX_BACKOFF},
    name="Indexer",
    daemon=True   # поставь False, если хочешь дождаться индексатора перед выходом процесса
)

thread_1.start()
thread_2.start()
thread_3.start()
thread_4.start()