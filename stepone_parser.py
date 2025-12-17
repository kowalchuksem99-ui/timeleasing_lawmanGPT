import os
import re
import time
import random
import logging
import csv
from datetime import date, timedelta
import json
import shutil
from contextlib import suppress
from typing import Optional, Set
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, WebDriverException

# ================== НАСТРОЙКИ ==================
BASE_URL = "https://kad.arbitr.ru/"
DOWNLOADS_WORK  = r"C:\_kad_cache\2024"   # локальный кэш
DOWNLOADS_FINAL = r"N:\kad_arbitr\2024"   # итоговая папка (как у вас сейчас)
MANIFEST_DIR = DOWNLOADS_FINAL

os.makedirs(DOWNLOADS_WORK, exist_ok=True)
os.makedirs(DOWNLOADS_FINAL, exist_ok=True)
os.makedirs(MANIFEST_DIR, exist_ok=True)


CASE_NO_RE = re.compile(r"(?:[АA]\d{1,3}-\d{1,7}/\d{4}"  # А40-12345/2020
                        r"|СИП-\d{1,7}(?:[-/]\d{4})?"  # СИП-715/2022 или СИП-715-2022
                        r"|SIP-\d{1,7}(?:[-/]\d{4})?"  # SIP-6-2020
                        r")", re.IGNORECASE)
INVALID_FS = '<>:"/\\|?*'

START_DATE = date(2024, 1, 1)  # 01.01.TARGET_YEAR
END_DATE = date(2024, 12, 31)  # 31.12.TARGET_YEAR
END_PAGE = 40  # максимум страниц на дату

START_DL_TIMEOUT = 45  # ждать старта скачивания (.crdownload/.pdf появился)
PER_FILE_TIMEOUT = 600  # максимум на один файл (до 10 минут)
STALL_TIMEOUT = 40  # стагнация размера .crdownload (сек) => считаем, что зависло

COURTS_FILE = "courts.txt"  # список судов, по одному в строке

# ИСКЛЮЧИТЬ суды
EXCLUDED_KEYWORDS = {
    "FASSKO", "ASKB", "ASKCHR", "ADYG",
    "MAHACHKALA", "INGUSHETIA", "KALMYK",
    "ALANIA", "CHECHNYA",
}
EXCLUDED_NAMES = {
    "АС Северо-Кавказского округа",
    "АС Кабардино-Балкарской Республики",
    "АС Карачаево-Черкесской Республики",
    "АС Республики Адыгея",
    "АС Республики Дагестан",
    "АС Республики Ингушетия",
    "АС Республики Калмыкия",
    "АС Республики Северная Осетия",
    "АС Чеченской Республики",
}

DOWNLOAD_ALL_CASES = False

# === ЛИЗИНГ-ФИЛЬТР: STRICT + NEAR ===========================================
STRICT_PATTERNS = [
    re.compile(r"\bлизинг(?:у|ом|а|е|и)?\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bлизингов\w+\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bлизингодател[ья][а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bлизингополучател[ья][а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bсублизинг\w*\b", re.IGNORECASE | re.UNICODE),

    re.compile(r"\bвыкупн\w+\s+лизинг\w*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bоперационн\w+\s+лизинг\w*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bвозвратн\w+\s+лизинг\w*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bфинанс\w+\s+аренд\w+\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bleasing\b", re.IGNORECASE | re.UNICODE),

    re.compile(r"\bдоговор(?:у|ом|а)?\s+лизинг[а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bпо\s+договор(?:у|ом|а)?\s+финанс\w+\s+аренд\w+\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bпредмет\s+лизинг[а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bлизингов\w+\s+платеж[а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"(?:\bизменен\w+|\bрасторжен\w+|\bотказ\s+от\s+приемк[иы])\s+(?:договор(?:у|ом|а)?\s+)?лизинг[а-я]*\b",
               re.IGNORECASE | re.UNICODE),
    re.compile(r"\bизъятие\s+предмет[а-я]*\s+лизинг[а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bпоручител[ья][а-я]*\s+по\s+договор(?:у|ом|а)?\s+лизинг[а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bсоотнесен\w+\s+встречн\w+\s+предоставлен\w+\s+сторон\s+по\s+договор(?:у|ом|а)?\s+лизинг[а-я]*\b",
               re.IGNORECASE | re.UNICODE),
    re.compile(r"\bаванс\w+\s+платеж\w+\s+(?:по\s+)?договор(?:у|ом|а)?\s+лизинг[а-я]*\b", re.IGNORECASE | re.UNICODE),
    re.compile(r"\bвозврат\s+аванс\w+\s+платеж[а-я]*\s+(?:по\s+)?договор(?:у|ом|а)?\s+лизинг[а-я]*\b",
               re.IGNORECASE | re.UNICODE),

    # Нормативные/обзорные якоря:
    re.compile(r"постановлен\w*\s+пленум[а-я]*\s+(?:вас|высш\w+\s+арбитраж\w+\s+суд)\s+рф[^0-9]{0,40}(?:№|n)\s*17\b",
               re.IGNORECASE | re.UNICODE),
    re.compile(
        r"обзор\s+судебн\w+\s+практик\w+[^()]*\(утв\.\s*президиумом\s+верховн\w+\s+суд[а-я]*\s+рф\s*27\.10\.2021",
        re.IGNORECASE | re.UNICODE),
]

NEAR_PATTERNS = [
    re.compile(r"\bсальдо\s+встречн\w+\s+обязательств\b", re.IGNORECASE | re.UNICODE),
]

ANCHOR_RE = re.compile(r"лизинг\w*|финанс\w+\s+аренд\w+", re.IGNORECASE | re.UNICODE)
NEAR_WINDOW = 120  # радиус в символах для связи нейтральных выражений с "лизинг"


def _normalize_text(s: str) -> str:
    # чтобы формы с "ё" не терялись
    return (s or "").replace("ё", "е").replace("Ё", "Е")


def _strict_hit(text: str) -> Optional[str]:
    for pat in STRICT_PATTERNS:
        m = pat.search(text)
        if m:
            return m.group(0)
    return None


def _near_hit(text: str) -> Optional[str]:
    for pat in NEAR_PATTERNS:
        for m in pat.finditer(text):
            start = max(0, m.start() - NEAR_WINDOW)
            end = m.end() + NEAR_WINDOW
            if ANCHOR_RE.search(text[start:end]):
                return m.group(0)
    return None


# ===========================================================================


WAIT_SHORT = 5
WAIT_MED = 15
WAIT_LONG = 60
DAILY_BREAK_HOURS = 5
_DAILY_BREAK_DATE: Optional[date] = None  # дата последней суточной паузы
PDF_DELAY_RANGE = (9, 15)  # пауза между скачиваниями (сек)
PDF_RETRY_BACKOFFS = (20, 45)  # пауза перед повторной попыткой (сек)
PDF_429_COOLDOWN = 180  # длинный откат при 429 (сек)
SETTLE_SEC = 3
OPEN_JITTER = (5, 9)
WAIT_DOWNLOADS = True
DEBUG_DIR = "debug_snapshots"
# ===============================================

os.makedirs(DEBUG_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CASE_LINK_XPATH = "//a[contains(@class,'num_case') and contains(@href,'/Card/')]"
PDF_LINK_XPATH = "//h2[contains(@class,'b-case-result')]//a[contains(@href,'.pdf')]"
PAGES_CONTAINER = "//*[@id='pages']"

# 2) добавьте вспомогательную функцию: берём блок после заголовка «Истцы/Истец/Заявитель…»
ROLE_WORDS = ("истец", "истцы", "заявитель", "заявители", "ответчик", "ответчики", "третьи лица", "иные лица")
SEP_RE = re.compile(r"\s*(?:;|,|•|·|—|-|\||/|\r?\n|\s{2,})\s*")


def _grab_party_block(txt: str, headers: tuple) -> str:
    stop = r"(?:Истец|Истцы|Заявитель|Заявители|Ответчик|Ответчики|Третьи лица|Иные лица|Суды и судьи|Судебные акты|Календарь|Электронное дело|$)"
    start = "|".join(headers)
    # допускаем в качестве границ не только перевод строки
    m = re.search(
        rf"(?:^|\r?\n|\s{{2,}})(?:{start})\s*:?\s*(.+?)(?=(?:\r?\n|\s{{2,}}|[|•·—-]\s*){stop})",
        txt, re.I | re.S
    )
    block = m.group(1) if m else ""
    parts = [p.strip() for p in SEP_RE.split(block)
             if p.strip() and p.strip().lower() not in ROLE_WORDS]
    return "; ".join(dict.fromkeys(parts))


def _unique_join(items):
    seen, out = set(), []
    for s in items:
        s = _norm_ws(s)
        if s and s not in seen:
            seen.add(s);
            out.append(s)
    return "; ".join(out)


def _texts(driver, xp):
    els = driver.find_elements(By.XPATH, xp)
    return [e.text for e in els if (e.text or "").strip()]


def get_parties_from_dom(driver):
    # Явно ждём появление блока «Участники дела»
    WebDriverWait(driver, WAIT_LONG).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, "table.b-case-info"))
    )

    # 1) имена из ссылок (обычный случай)
    pl = _texts(driver, "//td[contains(@class,'plaintiffs')]//li/a[normalize-space()]")
    df = _texts(driver, "//td[contains(@class,'defendants')]//li/a[normalize-space()]")

    # 2) фолбэк: если вдруг ссылок нет — берём текст всего <li>
    if not pl:
        pl = _texts(driver, "//td[contains(@class,'plaintiffs')]//li")
    if not df:
        df = _texts(driver, "//td[contains(@class,'defendants')]//li")

    return _unique_join(pl), _unique_join(df)


def _norm_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())


def _sanitize_component(s: str, max_len=60) -> str:
    s = _norm_ws(s)
    s = s.translate({ord(ch): "_" for ch in INVALID_FS})
    s = s.rstrip(". ")[:max_len].rstrip(". ")
    return s or "NA"


def _visible_text(driver) -> str:
    try:
        return driver.execute_script("return document.body ? document.body.innerText : '';") or ""
    except Exception:
        return driver.page_source or ""


def wait_case_card_ready(driver, timeout=30):
    """
    Ждём, пока в карточке появятся ключевые блоки: номер дела/участники/ссылка на суд.
    """
    locators = [
        (By.XPATH, "//h1[contains(.,'Дело') or contains(.,'№') or contains(.,'ДЕЛО')]"),
        (By.XPATH, "//a[contains(@href,'/Court')]"),
        (By.XPATH, "//*[contains(@class,'case') and (contains(.,'Истец') or contains(.,'Ответчик'))]"),
    ]
    end = time.time() + timeout
    while time.time() < end:
        for by, xp in locators:
            try:
                if driver.find_elements(by, xp):
                    time.sleep(0.3)  # короткая стабилизация рендера
                    return True
            except Exception:
                pass
        time.sleep(0.2)
    return False


def _grab_party(label_re: str, text: str) -> str:
    """
    Берём значение после метки (с переносами строк), но останавливаемся
    перед следующей меткой типа Ответчик/Третье лицо/Судья/Состав суда и т.п.
    """
    pattern = (
        rf"{label_re}\s*:\s*"  # 'Истец:' или 'Ответчик:'
        rf"(.+?)"  # само значение (ленивый захват, включая переводы строк)
        rf"(?=\r?\n\s*(?:Истец|Заявитель|Ответчик|Третье лицо|Судья|Состав суда|Секретарь|Стороны|$))"
    )
    m = re.search(pattern, text, re.I | re.S)
    if not m:
        return ""
    val = _norm_ws(m.group(1))
    # иногда внутри может быть разделитель '—' или метки вроде '(заявитель)'
    val = re.sub(r"\s*\((?:заявитель|ответчик|третье лицо)[^)]+\)\s*", " ", val, flags=re.I)
    return val.strip()


def ensure_parties_visible(driver):
    # если вкладка есть — кликнем по ней
    tabs = driver.find_elements(By.XPATH, "//*[self::a or self::button][contains(.,'Участники дела')]")
    if tabs:
        driver.execute_script("arguments[0].click();", tabs[0])
        time.sleep(0.2)


def extract_case_meta(driver, forced_court: Optional[str] = None) -> dict:
    txt = _visible_text(driver)

    # № дела
    m_no = CASE_NO_RE.search(txt)
    case_no = m_no.group(0) if m_no else ""

    # Суд (скрейпим как раньше, но ниже можем переопределить)
    court = ""
    links = [e for e in driver.find_elements(By.XPATH, "//a[contains(@href,'/Court')]")
             if (e.text or "").strip()]
    if links:
        court = max((_norm_ws(e.text) for e in links), key=len, default="")
    if not court:
        m = re.search(r"(Арбитражн(?:ый|ого)\s+суд[^\n\r]+|Суд по интеллектуальным правам[^\n\r]*)", txt, re.I)
        court = _norm_ws(m.group(0)) if m else ""

    # Стороны — сначала из DOM
    ensure_parties_visible(driver)
    plaintiff, defendant = get_parties_from_dom(driver)

    # Фолбэк на текстовую выжимку
    if not (plaintiff or defendant):
        plaintiff = _grab_party_block(txt, ("Истец", "Истцы", "Заявитель", "Заявители"))
        defendant = _grab_party_block(txt, ("Ответчик", "Ответчики"))

    # >>> ключевая строка: если задан принудительный суд — используем его
    if forced_court:
        court = forced_court

    return {"case_no": case_no, "court": court, "plaintiff": plaintiff, "defendant": defendant}


def _pdf_paths() -> Set[str]:
    return {
        os.path.join(DOWNLOADS_WORK, f)
        for f in os.listdir(DOWNLOADS_WORK)
        if f.lower().endswith(".pdf")
    }

def _fs_snapshot_all() -> Set[str]:
    return {
        os.path.join(DOWNLOADS_WORK, f)
        for f in os.listdir(DOWNLOADS_WORK)
        if f.lower().endswith(".pdf") or f.lower().endswith(".crdownload")
    }


def close_chrome_warning_popup(driver):
    try:
        for xp in [
            "//*[contains(.,'Уважаемые пользователи') or contains(.,'браузере Google Chrome')]",
            "//div[contains(@class,'modal') or contains(@class,'popup') or contains(@id,'overlay')]",
        ]:
            popups = [p for p in driver.find_elements(By.XPATH, xp) if p.is_displayed()]
            for p in popups:
                txt = (p.text or "").lower()
                if 'chrome' in txt or 'уважаемые' in txt:
                    logging.warning("Обнаружено системное уведомление о Chrome — закрываем.")
                    # сначала пробуем кнопку
                    btns = p.find_elements(By.XPATH, ".//button[contains(.,'Закрыть') or contains(.,'ОК')]")
                    if btns:
                        driver.execute_script("arguments[0].click();", btns[0])
                        logging.info("Chrome-предупреждение закрыто (кнопкой).")
                        time.sleep(0.3)
                        return True
                    # если нет кнопки — просто удаляем overlay
                    driver.execute_script("arguments[0].remove();", p)
                    logging.info("Chrome-предупреждение удалено из DOM.")
                    return True
    except Exception as e:
        logging.debug(f"Попытка закрыть Chrome-предупреждение: {e}")
    return False




def _wait_download_started(prev_all: Set[str], timeout: int = START_DL_TIMEOUT) -> Optional[str]:
    """Ждём появления НОВОГО .crdownload или .pdf; возвращаем найденный путь."""
    end = time.time() + timeout
    while time.time() < end:
        cur = _fs_snapshot_all()
        new = cur - prev_all
        if new:
            # безопасно получить время модификации (файл может уже исчезнуть)
            def safe_mtime(p):
                try:
                    return os.path.getmtime(p)
                except FileNotFoundError:
                    return 0  # считаем нулевым, чтобы не падало
            try:
                return max(new, key=safe_mtime)
            except ValueError:
                # если вдруг все кандидаты исчезли — продолжаем цикл
                pass
        time.sleep(0.2)
    return None



def _wait_download_completed(path: str, timeout: int = PER_FILE_TIMEOUT, stall: int = STALL_TIMEOUT) -> Optional[str]:
    # Уже готовый pdf?
    if path.lower().endswith(".pdf"):
        return path if os.path.isfile(path) else None

    if not path.lower().endswith(".crdownload"):
        return None

    base_pdf = path[:-len(".crdownload")]
    last_size = None
    last_change = time.time()
    end = time.time() + timeout

    while time.time() < end:
        pdf_exists = os.path.isfile(base_pdf)
        cr_exists  = os.path.isfile(path)

        # Нормальное завершение: .pdf есть, .crdownload нет
        if pdf_exists and not cr_exists:
            return base_pdf

        if cr_exists:
            # .crdownload ещё растёт — следим за размером с защитой от гонок
            try:
                sz = os.path.getsize(path)
            except FileNotFoundError:
                # Файл исчез между exists() и getsize(): дайте SMB проявиться
                time.sleep(0.3)
                # Если прямо сейчас увидим готовый .pdf — считаем успехом
                if os.path.isfile(base_pdf):
                    return base_pdf
                continue

            if last_size != sz:
                last_size = sz
                last_change = time.time()
            elif time.time() - last_change > stall:
                # давно не растёт → зависло/переименовалось незаметно
                return base_pdf if os.path.isfile(base_pdf) else None
        else:
            # На сетевых дисках .crdownload может исчезнуть раньше, чем .pdf станет виден
            time.sleep(0.3)
            if os.path.isfile(base_pdf):
                return base_pdf

        time.sleep(0.5)

    # На таймауте — если готовый .pdf всё-таки появился, отдадим его
    return base_pdf if os.path.isfile(base_pdf) else None



def _rename_pdf(path: str, meta: dict, seq: int) -> str:
    case = _sanitize_component(meta.get("case_no") or "без_номера", 40)
    court = _sanitize_component(meta.get("court") or "суд_не_извлечён", 70)
    pl = _sanitize_component(meta.get("plaintiff") or "истец_NA", 70)
    df = _sanitize_component(meta.get("defendant") or "ответчик_NA", 70)

    base_name = f"{case} — {court} — {pl} — {df} — {seq:02d}.pdf"
    dest_path = os.path.join(DOWNLOADS_FINAL, base_name)

    # Разруливаем дубликаты в конечной папке
    dup = 2
    stem = base_name[:-4]
    while os.path.exists(dest_path):
        dest_path = os.path.join(DOWNLOADS_FINAL, f"{stem} ({dup}).pdf")
        dup += 1

    try:
        # Если файл уже .pdf в рабочей папке — переносим
        if os.path.isfile(path):
            shutil.move(path, dest_path)
            logging.info(f"Перенесён: {os.path.basename(path)} → {dest_path}")
            return dest_path
        else:
            logging.warning(f"Исходный файл для переноса не найден: {path}")
            return path
    except Exception as e:
        logging.warning(f"Не удалось перенести '{path}' → '{dest_path}': {e}")
        return path


# ====== БРАУЗЕР ======
def setup_browser(chromedriver_path: Optional[str] = None,
                  chrome_binary: Optional[str] = None,
                  headless: bool = False):
    os.makedirs(DOWNLOADS_WORK, exist_ok=True)

    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-pdf-viewer")  # чтобы не перехватывал просмотрщик
    options.page_load_strategy = "eager"
    options.add_argument("--ignore-certificate-errors")
    options.add_argument("--ignore-ssl-errors=yes")
    options.add_argument("--allow-running-insecure-content")

    if headless:
        options.add_argument("--headless=new")

    # 1) можно зафиксировать конкретный Chrome (portable) через параметр или переменную окружения
    chrome_binary = chrome_binary or os.getenv("CHROME_BINARY")
    if chrome_binary:
        options.binary_location = chrome_binary

    # 2) можно зафиксировать конкретный chromedriver через параметр или переменную окружения
    chromedriver_path = chromedriver_path or os.getenv("CHROMEDRIVER")
    service = ChromeService(executable_path=chromedriver_path) if chromedriver_path else ChromeService()

    prefs = {
        "download.default_directory": DOWNLOADS_WORK,
        "download.prompt_for_download": False,
        "safebrowsing.enabled": True,
        "plugins.always_open_pdf_externally": True,
        "download.directory_upgrade": True,
    }
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(options=options, service=service)

    # Разрешаем загрузки через CDP (актуально для chrome >= 76)
    with suppress(Exception):
        driver.execute_cdp_cmd("Page.setDownloadBehavior", {
            "behavior": "allow",
            "downloadPath": DOWNLOADS_WORK
        })

    # Логируем фактические версии браузера и драйвера
    try:
        caps = driver.capabilities or {}
        browser_ver = caps.get("browserVersion") or caps.get("version")
        cd_ver = (caps.get("chrome", {}) or {}).get("chromedriverVersion", "")
        logging.info(f"Chrome: {browser_ver} | ChromeDriver: {cd_ver}")
    except Exception:
        pass

    logging.info("Браузер запущен и настроен")
    return driver


def save_debug_artifacts(driver, prefix):
    ts = time.strftime("%Y%m%d_%H%M%S")
    try:
        driver.save_screenshot(os.path.join(DEBUG_DIR, f"{prefix}_{ts}.png"))
    except Exception:
        pass
    try:
        with open(os.path.join(DEBUG_DIR, f"{prefix}_{ts}.html"), "w", encoding="utf-8") as f:
            f.write(driver.page_source)
    except Exception:
        pass


def _rate_sleep(sec: float, reason: str = "анти-429"):
    sec = float(sec)
    logging.info(f"Пауза {sec:.1f} сек ({reason}).")
    time.sleep(sec)


def daily_pause_if_needed():
    """Раз в календарные сутки делаем паузу на DAILY_BREAK_HOURS часов."""
    global _DAILY_BREAK_DATE
    today = date.today()
    if _DAILY_BREAK_DATE is None:
        _DAILY_BREAK_DATE = today  # при первом запуске паузу не делаем
        return
    if today != _DAILY_BREAK_DATE:
        logging.warning(f"Ежедневный перерыв на {DAILY_BREAK_HOURS} ч (имитация человека).")
        _rate_sleep(DAILY_BREAK_HOURS * 3600, "ежедневная пауза")
        _DAILY_BREAK_DATE = date.today()


def _random_pdf_delay():
    _rate_sleep(random.uniform(*PDF_DELAY_RANGE), "между скачиваниями")


def _looks_rate_limited(driver) -> bool:
    """Грубая эвристика: title/body содержит 429/Too Many Requests/«слишком много запросов»."""
    try:
        title = (driver.title or "").lower()
    except Exception:
        title = ""
    try:
        body = driver.execute_script("return document.body ? document.body.innerText : '';") or ""
    except Exception:
        body = ""
    txt = (title + " " + body).lower()
    return (
            "429" in txt
            or "too many requests" in txt
            or "слишком много запросов" in txt
            or "превышено количество запросов" in txt
    )


def is_leasing_case(driver) -> bool:
    """
    True, если карточка дела относится к лизингу:
      1) STRІCT — паттерны, самодостаточные (включая "финансовая аренда", №17 и Обзор 27.10.2021);
      2) NEAR — нейтральные словосочетания (например, "сальдо встречных обязательств"),
         засчитываем только если в окрестности ±NEAR_WINDOW встречается 'лизинг'/'финансовая аренда'.
    """
    try:
        raw = driver.execute_script("return document.body ? document.body.innerText : '';") or ""
    except Exception:
        raw = driver.page_source or ""
    text = _normalize_text(raw)

    hit = _strict_hit(text)
    if hit:
        logging.info(f"ЛИЗИНГ: STRICT → «{hit[:80]}»")
        return True

    near = _near_hit(text)
    if near:
        logging.info(f"ЛИЗИНГ: NEAR → «{near[:80]}» (окно ±{NEAR_WINDOW})")
        return True

    logging.info("ЛИЗИНГ: совпадений не найдено.")
    return False


# ====== ХЕЛПЕРЫ UI ======
def close_popup_if_present(driver):
    try:
        btn = WebDriverWait(driver, 3).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "js-promo_notification-popup-close"))
        )
        btn.click()
        logging.info("Промо-попап закрыт")
    except TimeoutException:
        pass
    for sel in [
        "//button[contains(@class,'cookie') and (contains(.,'Согласен') or contains(.,'Принять'))]",
        "//button[contains(.,'Согласен')]",
        "//button[contains(.,'Принять')]",
    ]:
        try:
            btn = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.XPATH, sel)))
            btn.click()
            logging.info("Cookie-попап закрыт")
            break
        except TimeoutException:
            continue


def clear_and_type(element, text):
    element.click()
    element.send_keys(Keys.CONTROL, "a")
    element.send_keys(Keys.BACKSPACE)
    if text:
        element.send_keys(text)


def wait_results_stable(driver, timeout=WAIT_LONG, settle=SETTLE_SEC):
    end_time = time.time() + timeout
    last = -1
    stable_since = time.time()
    while time.time() < end_time:
        links = driver.find_elements(By.XPATH, CASE_LINK_XPATH)
        count = len(links)
        nothing = driver.find_elements(By.XPATH,
                                       "//*[contains(.,'Ничего не найдено') or contains(.,'По вашему запросу ничего не найдено')]")
        if count > 0:
            if count != last:
                last = count
                stable_since = time.time()
            elif time.time() - stable_since >= settle:
                return count
        elif nothing:
            return 0
        time.sleep(0.25)
    logging.warning("Таймаут ожидания результатов — продолжаем с тем, что есть")
    return len(driver.find_elements(By.XPATH, CASE_LINK_XPATH))


def wait_downloads_finished(folder, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        cr = [f for f in os.listdir(folder) if f.endswith(".crdownload")]
        if not cr:
            return True
        time.sleep(0.5)
    logging.warning("Не дождались окончания загрузок — продолжаем")
    return False


def first_case_href(driver):
    try:
        el = driver.find_elements(By.XPATH, CASE_LINK_XPATH)
        return el[0].get_attribute("href") if el else None
    except StaleElementReferenceException:
        return None


def go_to_page(driver, target_page_num, prev_first_href=None):
    try:
        pages = WebDriverWait(driver, WAIT_SHORT).until(
            EC.presence_of_element_located((By.XPATH, PAGES_CONTAINER))
        )
        driver.execute_script("arguments[0].scrollIntoView({block:'end'});", pages)
        time.sleep(0.2)
        link = driver.find_elements(By.XPATH, f"{PAGES_CONTAINER}//a[normalize-space(text())='{target_page_num}']")
        if not link:
            link = driver.find_elements(By.XPATH,
                                        f"{PAGES_CONTAINER}//a[contains(@class,'next') or contains(.,'›') or contains(.,'След')]")
        if not link:
            return False
        driver.execute_script("arguments[0].click();", link[0])
        if prev_first_href:
            WebDriverWait(driver, WAIT_LONG).until(
                lambda d: first_case_href(d) not in (None, prev_first_href)
            )
        wait_results_stable(driver)
        return True
    except TimeoutException:
        return False


# --- helpers for robust pagination ----
def page_signature(driver, k=5):
    """Кортеж первых k href дел на странице (для детекции смены страницы)."""
    hrefs = []
    els = driver.find_elements(By.XPATH, CASE_LINK_XPATH)
    for e in els[:k]:
        try:
            hrefs.append(e.get_attribute("href") or "")
        except StaleElementReferenceException:
            hrefs.append("")
    return tuple(hrefs)


def wait_results_changed(driver, prev_sig, timeout=WAIT_LONG):
    """Ждём, пока список дел реально поменяется (по сигнатуре), а не мигнёт."""
    end = time.time() + timeout
    while time.time() < end:
        # короткая стабилизация текущего рендера
        wait_results_stable(driver, timeout=WAIT_MED, settle=0.6)
        sig = page_signature(driver)
        # должны отличаться и содержать хотя бы один непустой href
        if sig and sig != prev_sig and any(sig):
            return True
        time.sleep(0.2)
    save_debug_artifacts(driver, "pagination_wait_timeout")
    return False


def get_active_page(driver):
    """Возвращает номер активной страницы: <li><span>n</span></li> без ссылки — основной кейс."""
    xps = [
        "((//*[@id='pages' or @id='pagesTop']//li[span and not(a)]/span)[1])",  # span без ссылки
        "((//*[@id='pages' or @id='pagesTop']//a[@aria-current='page'])[1])",  # aria-current
        ("((//*[@id='pages' or @id='pagesTop']"
         "//li[contains(@class,'active') or contains(@class,'selected') or contains(@class,'current')]"
         "//*[self::a or self::span])[1])"),  # классы
    ]
    for xp in xps:
        try:
            el = driver.find_element(By.XPATH, xp)
            txt = (el.text or "").strip()
            num = int("".join(ch for ch in txt if ch.isdigit())) if txt else None
            if num:
                return num
        except Exception:
            continue
    return None


def _find_next_link_element(driver):
    """
    Находим ссылку на СЛЕДУЮЩУЮ страницу относительно активной.
    Возвращаем (element, cur_num). Используем только как фолбэк.
    """
    # контейнеры пагинации сверху/снизу/по классам
    containers = []
    for xp in ("//*[@id='pages']", "//*[@id='pagesTop']",
               "//*[contains(@class,'pages') or contains(@class,'paging') or contains(@class,'pager')]"):
        containers.extend([e for e in driver.find_elements(By.XPATH, xp) if e.is_displayed()])

    for cont in containers:
        # активный <li> (обычно span без a)
        active_li = None
        for xp in (".//li[span and not(a)]",
                   ".//li[contains(@class,'active') or contains(@class,'selected') or contains(@class,'current')]"):
            found = cont.find_elements(By.XPATH, xp)
            if found:
                active_li = found[0];
                break
        if not active_li:
            continue

        # пытаемся прочитать текущий номер
        try:
            cur_txt = (active_li.text or "").strip()
            cur_num = int("".join(ch for ch in cur_txt if ch.isdigit())) if cur_txt else None
        except Exception:
            cur_num = None

        # ближайший следующий <li><a>
        nxt = active_li.find_elements(By.XPATH, "following-sibling::li[a][1]/a")
        if nxt:
            return nxt[0], cur_num

        # через '…'
        ell = active_li.find_elements(By.XPATH, "following-sibling::li[.='…' or .='...'][1]")
        if ell:
            nxt = ell[0].find_elements(By.XPATH, "following-sibling::li[a][1]/a")
            if nxt:
                return nxt[0], cur_num

    return None, None


def click_page_number(driver, n: int) -> bool:
    """Надёжно кликаем по ссылке с номером n (ищем в обоих пейджерах)."""
    xps = [
        f"//*[@id='pages']//a[normalize-space(text())='{n}']",
        f"//*[@id='pagesTop']//a[normalize-space(text())='{n}']",
        ("//*[contains(@class,'pages') or contains(@class,'paging') or contains(@class,'pager')]"
         f"//a[normalize-space(text())='{n}']"),
    ]
    for xp in xps:
        links = [a for a in driver.find_elements(By.XPATH, xp) if a.is_displayed()]
        if not links:
            continue
        el = links[0]
        try:
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
            time.sleep(0.05)
            driver.execute_script("arguments[0].click();", el)  # 1) JS-клик
            return True
        except Exception:
            pass
        try:
            ActionChains(driver).move_to_element(el).pause(0.05).click().perform()  # 2) реальный клик
            return True
        except Exception:
            pass
        try:
            el.click()  # 3) нативный клик
            return True
        except Exception:
            continue
    return False


def click_next_button(driver):
    """Кликаем по 'Следующая'/'›' если доступна."""
    for xp in [
        f"{PAGES_CONTAINER}//a[contains(@class,'next')]",
        f"{PAGES_CONTAINER}//*[self::a or self::button][contains(.,'След') or contains(.,'›')]",
    ]:
        btns = [b for b in driver.find_elements(By.XPATH, xp) if b.is_displayed() and b.is_enabled()]
        if btns:
            driver.execute_script("arguments[0].click();", btns[0])
            return True
    return False


def go_to_next_page(driver, prev_sig):
    """
    Переходим на следующую страницу БЕЗ перепрыгов:
    1) сначала ищем и кликаем ТОЛЬКО по ссылке с текстом cur+1 (в любом пейджере);
    2) если такой ссылки нет — используем соседа, НО только если он действительно == cur+1;
    3) валидируем, что активная страница стала cur+1; если нет — повторная попытка кликнуть ровно cur+1.
    """
    # показать пейджеры
    for xp in ("//*[@id='pagesTop']", "//*[@id='pages']"):
        try:
            el = driver.find_element(By.XPATH, xp)
            driver.execute_script("arguments[0].scrollIntoView({block:'end'});", el)
            time.sleep(0.1)
        except Exception:
            pass

    cur = get_active_page(driver) or 1
    target = cur + 1
    logging.info(f"Пейджер: активная {cur}, целевая {target}")

    # 1) базовый путь — клик по точному номеру (любой контейнер)
    if not click_page_number(driver, target):
        # 2) фолбэк — сосед после активного, но проверяем, что он == target
        neigh_el, cur_num = _find_next_link_element(driver)
        if not neigh_el:
            return False
        try:
            txt = (neigh_el.text or "").strip()
            neigh_num = int("".join(ch for ch in txt if ch.isdigit())) if txt else None
        except Exception:
            neigh_num = None
        if neigh_num != target:
            # сосед ведёт не на target — лучше не рисковать перепрыгом
            return False

        # кликаем соседний (= target)
        for click in (
                lambda: driver.execute_script("arguments[0].click();", neigh_el),
                lambda: ActionChains(driver).move_to_element(neigh_el).pause(0.05).click().perform(),
                lambda: neigh_el.click(),
        ):
            try:
                click();
                break
            except Exception:
                continue
        else:
            save_debug_artifacts(driver, "pagination_click_failed")
            return False

    # 3) ждём ровно target; если страница изменилась, но номер не тот — пробуем скорректировать
    end = time.time() + WAIT_LONG
    while time.time() < end:
        now = get_active_page(driver)
        if now == target:
            break
        # если контент уже другой, а номер не target — корректируем точным кликом
        if page_signature(driver) != prev_sig:
            if click_page_number(driver, target):
                # ещё немного подождём активацию target
                WebDriverWait(driver, WAIT_MED).until(lambda d: get_active_page(d) == target)
                break
        time.sleep(0.2)
    else:
        save_debug_artifacts(driver, "pagination_not_changed_to_target")
        return False

    wait_results_stable(driver)
    return True


def collect_case_links(driver):
    wait_results_stable(driver)
    links = [a.get_attribute("href") for a in driver.find_elements(By.XPATH, CASE_LINK_XPATH)]
    return [u for u in links if u]


def open_in_new_tab(driver, url) -> str:
    """
    Открывает url в новой вкладке.
    Возвращает handle вкладки, к которой нужно вернуться (т.е. предыдущую).
    """
    back_handle = driver.current_window_handle
    before = set(driver.window_handles)
    driver.execute_script("window.open(arguments[0], '_blank');", url)
    WebDriverWait(driver, WAIT_LONG).until(
        lambda d: len(set(d.window_handles) - before) == 1
    )
    new_handle = (set(driver.window_handles) - before).pop()
    driver.switch_to.window(new_handle)
    return back_handle


def close_and_back(driver, back_handle: str):
    """Закрыть текущую вкладку и вернуться на заданную (если жива)."""
    try:
        driver.close()
    finally:
        if back_handle in driver.window_handles:
            driver.switch_to.window(back_handle)
        else:
            # если по какой-то причине исходная вкладка исчезла — идём в последнюю живую
            driver.switch_to.window(driver.window_handles[-1])


def close_current_tab_and_back(driver):
    driver.close()
    driver.switch_to.window(driver.window_handles[0])


MANIFEST_EXT = ".manifest.json"


def _safe_case_for_prefix(case_no: str) -> str:
    # тем же санитайзером, что и для имени файла
    return _sanitize_component(case_no or "без_номера", 40)


def _manifest_path(case_no: str) -> str:
    safe = _safe_case_for_prefix(case_no)
    return os.path.join(MANIFEST_DIR, f"{safe}{MANIFEST_EXT}")


def _write_manifest(case_no: str, data: dict):
    with open(_manifest_path(case_no), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)


def _read_manifest(case_no: str) -> dict:
    with suppress(Exception):
        with open(_manifest_path(case_no), "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def scrape_case_pdfs(driver, meta: dict):
    case_handle = driver.current_window_handle
    pdf_links = [a.get_attribute("href") for a in driver.find_elements(By.XPATH, PDF_LINK_XPATH)]
    pdf_links = [u for u in pdf_links if u]
    logging.info(f"Найдено PDF: {len(pdf_links)}")

    case_no = meta.get("case_no") or ""
    safe_case = _safe_case_for_prefix(case_no)

    manifest = {
        "case_no": case_no,
        "safe_case": safe_case,
        "expected": len(pdf_links),
        "have": 0,
        "status": "downloading",
        "court": meta.get("court"),
        "plaintiff": meta.get("plaintiff"),
        "defendant": meta.get("defendant"),
        "ts": time.time(),
    }
    _write_manifest(case_no, manifest)

    seq = 1
    failed = []

    def _try_one(pdf_url: str, attempt: int) -> bool:
        # Пауза перед каждой попыткой скачивания
        _random_pdf_delay()

        logging.info(f"[PDF attempt {attempt}] Открываем: {pdf_url}")
        prev_all = _fs_snapshot_all()

        back = open_in_new_tab(driver, pdf_url)
        try:
            # ждём старта загрузки
            started = _wait_download_started(prev_all, timeout=START_DL_TIMEOUT)
            if not started:
                # если страта нет, посмотрим, не rate-limit ли
                if _looks_rate_limited(driver):
                    logging.warning("Похоже, словили 429 (старта нет). Делаем длительный откат.")
                    _rate_sleep(PDF_429_COOLDOWN, "cooldown после 429")
                else:
                    # мягкий бэкофф на любую неудачу старта
                    _rate_sleep(random.uniform(*PDF_RETRY_BACKOFFS), "бэкофф (старта нет)")
                return False

            logging.info(f"Старт: {os.path.basename(started)}")

            # ждём завершения
            try:
                final_pdf = _wait_download_completed(started, timeout=PER_FILE_TIMEOUT, stall=STALL_TIMEOUT)
            except Exception as e:
                logging.exception(f"Ошибка внутри _wait_download_completed: {e}")
                final_pdf = None
            if not final_pdf:
                # если подвисло/таймаут — возможно, тоже rate-limit
                if _looks_rate_limited(driver):
                    logging.warning("Похоже, 429 в процессе загрузки. Делаем длительный откат.")
                    _rate_sleep(PDF_429_COOLDOWN, "cooldown после 429")
                else:
                    _rate_sleep(random.uniform(*PDF_RETRY_BACKOFFS), "бэкофф (таймаут/стагнация)")
                return False

            # успешно — переименуем
            _rename_pdf(final_pdf, meta, seq)
            return True

        finally:
            close_and_back(driver, back)

    # Первый проход
    for pdf_url in pdf_links:
        ok = _try_one(pdf_url, attempt=1)
        if not ok:
            failed.append(pdf_url)
        else:
            manifest["have"] += 1
            _write_manifest(case_no, manifest)
            seq += 1

    # Повторный проход по неудавшимся
    if failed:
        logging.info(f"Повторная попытка: {len(failed)} файл(ов)")
        retry = []
        for pdf_url in failed:
            ok = _try_one(pdf_url, attempt=2)
            if ok:
                manifest["have"] += 1
                _write_manifest(case_no, manifest)
                seq += 1
            else:
                retry.append(pdf_url)
        failed = retry

    # На всякий случай — дождаться исчезновения .crdownload
    wait_downloads_finished(DOWNLOADS_WORK)

    manifest["status"] = "complete" if manifest["have"] >= manifest["expected"] else "partial"
    manifest["ts_done"] = time.time()
    _write_manifest(case_no, manifest)

    if failed:
        logging.warning(f"Не скачались {len(failed)} файла(ов). Примеры: {failed[:2]}")


# ====== ПОЛЕ «СУД» (точно не «Судья») ======
def find_court_input(driver):
    """
    Ищем ИНПУТ именно 'Суд':
    1) точный плейсхолдер 'название суда' (уникален),
    2) плейсхолдер вида 'название ... суд' (регистронезависимо),
    3) инпут в одном блоке с заголовком 'Суд' (не 'Судья').
    """
    # 1) самый надёжный — точный плейсхолдер
    xp1 = "//input[@placeholder='название суда']"
    els = driver.find_elements(By.XPATH, xp1)
    for e in els:
        if e.is_displayed() and e.is_enabled():
            return e

    # 2) 'название ... суд' (регистронезависимо)
    xp2 = ("//input[not(@type='hidden') and "
           "contains(translate(@placeholder,'НАВЗИСУД','навзисуд'),'название') and "
           "contains(translate(@placeholder,'СУД','суд'),'суд')]")
    els = driver.find_elements(By.XPATH, xp2)
    for e in els:
        if e.is_displayed() and e.is_enabled():
            return e

    # 3) инпут рядом с заголовком 'Суд' (исключая 'Судья')
    xp3 = ("//div[.//*[normalize-space(text())='Суд' or starts-with(normalize-space(text()),'Суд')]]"
           "[not(.//*[contains(normalize-space(text()),'Судья')])]"
           "//input[not(@type='hidden')]")
    els = driver.find_elements(By.XPATH, xp3)
    for e in els:
        if e.is_displayed() and e.is_enabled():
            return e

    save_debug_artifacts(driver, "find_court_input_failed")
    raise TimeoutException("Инпут 'Суд' не найден (попробовал placeholder и блок по заголовку).")


def wait_suggestions_and_pick(driver, court_name):
    suggestion_xpath = (
        "//div[contains(@class,'ui-autocomplete') and not(contains(@style,'display: none'))]//li"
        " | //div[contains(@class,'suggest') and contains(@class,'popup') and not(contains(@style,'display: none'))]//li"
        " | //ul[contains(@class,'ui-menu') and not(contains(@style,'display: none'))]//li"
        " | //div[contains(@class,'autocomplete') and not(contains(@style,'display: none'))]//li"
    )
    WebDriverWait(driver, WAIT_LONG).until(
        EC.visibility_of_any_elements_located((By.XPATH, suggestion_xpath))
    )
    items = [it for it in driver.find_elements(By.XPATH, suggestion_xpath) if it.is_displayed()]
    if not items:
        save_debug_artifacts(driver, "court_suggest_empty")
        raise TimeoutException("Подсказки по суду не появились.")
    want = court_name.lower().strip()
    target = None
    for it in items:
        try:
            txt = (it.text or "").strip()
        except StaleElementReferenceException:
            continue
        low = txt.lower()
        if want in low or low in want:
            target = it
            break
    if target is None:
        target = items[0]
    driver.execute_script("arguments[0].scrollIntoView({block:'nearest'});", target)
    driver.execute_script("arguments[0].click();", target)


def court_selected(driver, court_name):
    try:
        inp = find_court_input(driver)
        val = (inp.get_attribute("value") or "").lower()
        if court_name.lower() in val:
            return True
        # иногда после выбора значение инпута очищается, но тег-чип с названием появляется рядом:
        block_text = inp.find_element(By.XPATH,
                                      "./ancestor::*[contains(@class,'field') or contains(@class,'sug') or self::div][1]").text.lower()
        if court_name.lower() in block_text:
            return True
    except Exception:
        pass
    return False


def clear_court_selection(driver):
    try:
        inp = find_court_input(driver)
        # пробуем «крестики» рядом с инпутом
        area = inp.find_element(By.XPATH, "./ancestor::*[self::div or self::section][1]")
        for xp in [
            ".//span[contains(@class,'remove') or contains(@class,'close') or contains(@class,'ui-icon-close')]",
            ".//a[contains(@class,'clear') or contains(@class,'remove')]",
            ".//button[contains(@class,'clear') or contains(@class,'remove')]",
        ]:
            for b in area.find_elements(By.XPATH, xp):
                try:
                    driver.execute_script("arguments[0].click();", b)
                    time.sleep(0.05)
                except Exception:
                    pass
        clear_and_type(inp, "")
        inp.send_keys(Keys.ESCAPE)
    except Exception:
        pass


def set_court(driver, court_name):
    clear_court_selection(driver)
    inp = find_court_input(driver)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", inp)
    WebDriverWait(driver, WAIT_LONG).until(EC.element_to_be_clickable(inp))
    clear_and_type(inp, court_name)
    try:
        wait_suggestions_and_pick(driver, court_name)
    except TimeoutException:
        inp.send_keys(Keys.ENTER)
    WebDriverWait(driver, WAIT_LONG).until(lambda d: court_selected(d, court_name))
    logging.info(f"Выбран суд: {court_name}")


# ====== ФИЛЬТРЫ И ПОИСК ======
def fill_date_input(driver, label_class, date_value):
    label = WebDriverWait(driver, WAIT_LONG).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, f"#sug-dates label.{label_class}"))
    )
    input_field = label.find_element(By.CSS_SELECTOR, "input.anyway_position_top")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", input_field)
    WebDriverWait(driver, WAIT_LONG).until(EC.element_to_be_clickable(input_field))
    clear_and_type(input_field, date_value)
    WebDriverWait(driver, WAIT_LONG).until(lambda d: input_field.get_attribute("value") == date_value)
    time.sleep(0.2)


def enter_filters(driver, court_name, start_date_str, end_date_str):
    driver.get(BASE_URL)
    close_popup_if_present(driver)
    close_chrome_warning_popup(driver)
    WebDriverWait(driver, WAIT_LONG).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
    time.sleep(0.8)
    close_popup_if_present(driver)
    close_chrome_warning_popup(driver)

    # --- выбираем суд ---
    close_chrome_warning_popup(driver)
    set_court(driver, court_name)

    # --- ждём, пока после выбора суда отрисуются поля дат ---
    try:
        WebDriverWait(driver, WAIT_LONG).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#sug-dates input.anyway_position_top"))
        )
    except TimeoutException:
        logging.warning("Поле даты не появилось после выбора суда — обновляем страницу.")
        driver.get(BASE_URL)
        close_popup_if_present(driver)
        close_chrome_warning_popup(driver)
        close_chrome_warning_popup(driver)
        set_court(driver, court_name)
        WebDriverWait(driver, WAIT_LONG).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#sug-dates input.anyway_position_top"))
        )

    # --- теперь задаём даты, с повторной проверкой значения ---
    for label_class, date_value in [("from", start_date_str), ("to", end_date_str)]:
        for attempt in range(3):
            try:
                fill_date_input(driver, label_class, date_value)
                # проверяем, что дата реально вписана
                val = driver.find_element(By.CSS_SELECTOR, f"#sug-dates label.{label_class} input").get_attribute("value")
                if val.strip() == date_value:
                    break
            except Exception as e:
                logging.warning(f"Повтор ввода даты {label_class} ({attempt+1}/3): {e}")
                time.sleep(0.5)
        else:
            logging.error(f"Не удалось вписать дату {label_class}={date_value}")

    # --- клик «Найти» ---
    driver.find_element(By.TAG_NAME, "body").click()
    time.sleep(0.2)
    search_button = WebDriverWait(driver, WAIT_LONG).until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(.,'Найти')]"))
    )
    driver.execute_script("arguments[0].click();", search_button)
    wait_results_stable(driver)



# ====== ИТЕРАЦИИ, ЗАГРУЗКА, СПИСОК СУДОВ ======
def generate_date_range(start: date, finish: date):
    cur = start
    while cur <= finish:
        yield cur.strftime("%d.%m.%Y")
        cur += timedelta(days=1)


def process_cases_for_date(driver, date_str, court_name: str):
    current_page = 1
    seen_sigs = set()

    while current_page <= END_PAGE:
        wait_results_stable(driver)

        results_handle = driver.current_window_handle
        sig = page_signature(driver)
        if sig in seen_sigs:
            logging.warning(f"[{date_str}] Повторилась та же страница (сигнатура). Останавливаемся.")
            break
        seen_sigs.add(sig)

        links = collect_case_links(driver)
        logging.info(f"[{date_str}] Страница {current_page}: дел найдено {len(links)}")
        if not links:
            break

        for idx, href in enumerate(links, 1):
            logging.info(f"Открываем дело {idx}/{len(links)}: {href}")
            back = open_in_new_tab(driver, href)
            try:
                time.sleep(random.uniform(*OPEN_JITTER))
                wait_case_card_ready(driver, timeout=WAIT_LONG)

                # if not is_leasing_case(driver):
                #    logging.info("Пропуск дела: лизинговая тематика не найдена.")
                #    continue
                if not DOWNLOAD_ALL_CASES:
                    if not is_leasing_case(driver):
                        logging.info("Пропуск дела: лизинговая тематика не найдена.")
                        continue

                wait_case_card_ready(driver, timeout=WAIT_LONG)
                ensure_parties_visible(driver)

                # >>> здесь принудительно прокидываем court_name
                meta = extract_case_meta(driver, forced_court=court_name)

                scrape_case_pdfs(driver, meta)

            finally:
                close_and_back(driver, back)

        if results_handle in driver.window_handles:
            driver.switch_to.window(results_handle)
        else:
            logging.error("Вкладка результатов неожиданно закрыта. Прерываем.")
            break

        prev_sig = page_signature(driver)
        if not go_to_next_page(driver, prev_sig):
            logging.info(f"[{date_str}] Нет следующей страницы или не удалось переключиться.")
            break

        current_page += 1


def load_courts_list():
    courts = []
    if os.path.isfile(COURTS_FILE):
        with open(COURTS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                name = line.strip()
                if not name:
                    continue
                courts.append(name)
    else:
        courts = [
            "15 арбитражный апелляционный суд",
            "16 арбитражный апелляционный суд",
            "17 арбитражный апелляционный суд",
            "18 арбитражный апелляционный суд",
            "19 арбитражный апелляционный суд",
            "20 арбитражный апелляционный суд",
            "21 арбитражный апелляционный суд",
            "АС Алтайского края",
            "АС Амурской области",
            "АС Архангельской области",
            "АС Астраханской области",
            "АС Белгородской области",
            "АС Брянской области",
            "АС Владимирской области",
            "АС Волгоградской области",
            "АС Вологодской области",
            "АС Воронежской области",
            "АС города Москвы",
            "АС города Санкт-Петербурга и Ленинградской области",
            "АС города Севастополя",
            "АС Донецкой Народной Республики",
            "АС Еврейской автономной области",
            "АС Забайкальского края",
            "АС Запорожской области",
            "АС Ивановской области",
            "АС Иркутской области",
            "АС Калининградской области",
            "АС Калужской области",
            "АС Камчатского края",
            "АС Кемеровской области",
            "АС Кировской области",
            "АС Коми-Пермяцкого АО",
            "АС Костромской области",
            "АС Краснодарского края",
            "АС Красноярского края",
            "АС Курганской области",
            "АС Курской области",
            "АС Липецкой области",
            "АС Луганской Народной Республики",
            "АС Магаданской области",
            "АС Московской области",
            "АС Мурманской области",
            "АС Нижегородской области",
            "АС Новгородской области",
            "АС Новосибирской области",
            "АС Омской области",
            "АС Оренбургской области",
            "АС Орловской области",
            "АС Пензенской области",
            "АС Пермского края",
            "АС Приморского края",
            "АС Псковской области",
            "АС Республики Алтай",
            "АС Республики Башкортостан",
            "АС Республики Бурятия",
            "АС Республики Карелия",
            "АС Республики Коми",
            "АС Республики Крым",
            "АС Республики Марий Эл",
            "АС Республики Мордовия",
            "АС Республики Саха",
            "АС Республики Татарстан",
            "АС Республики Тыва",
            "АС Республики Хакасия",
            "АС Ростовской области",
            "АС Рязанской области",
            "АС Самарской области",
            "АС Саратовской области",
            "АС Сахалинской области",
            "АС Свердловской области",
            "АС Смоленской области",
            "АС Ставропольского края",
            "АС Тамбовской области",
            "АС Тверской области",
            "АС Томской области",
            "АС Тульской области",
            "АС Тюменской области",
            "АС Удмуртской Республики",
            "АС Ульяновской области",
            "АС Хабаровского края",
            "АС Ханты-Мансийского АО",
            "АС Херсонской области",
            "АС Челябинской области",
            "АС Чувашской Республики",
            "АС Чукотского АО",
            "АС Ямало-Ненецкого АО",
            "АС Ярославской области",
            "ПСП Арбитражного суда Пермского края",
            "ПСП Арбитражный суд Архангельской области",
            "Суд по интеллектуальным правам",
        ]

    def is_excluded(name: str) -> bool:
        if any(ex in name for ex in EXCLUDED_NAMES):
            return True
        upper = name.upper()
        return any(tok in upper for tok in EXCLUDED_KEYWORDS)

    return [c for c in courts if not is_excluded(c)]


def STEP_ONE():
    driver = setup_browser(
        chromedriver_path=r"DRIVER PATH",
    )

    try:
        courts = load_courts_list()
        if not courts:
            logging.error("После фильтрации не осталось судов для обхода. Проверьте courts.txt и исключения.")
            return
        logging.info(f"Всего судов к обходу: {len(courts)}")
        for court in courts:
            logging.info(f"===== СТАРТ ПО СУДУ: {court} =====")
            for dstr in generate_date_range(START_DATE, END_DATE):
                try:
                    daily_pause_if_needed()
                    enter_filters(driver, court, dstr, dstr)
                    process_cases_for_date(driver, dstr, court_name=court)
                except (TimeoutException, WebDriverException) as e:
                    logging.exception(f"Ошибка на дате {dstr} для суда {court}: {e}")
                    save_debug_artifacts(driver, f"error_{court}_{dstr.replace('.', '-')}")
                    try:
                        driver.get("about:blank");
                        time.sleep(0.3)
                    except Exception:
                        pass
                    continue
            logging.info(f"===== ГОТОВО ПО СУДУ: {court} =====")
            time.sleep(random.uniform(2, 5))
    finally:
        try:
            driver.quit()
        except Exception:
            pass
        logging.info("Браузер закрыт. Работа завершена.")
    return None


STEP_ONE()
