from pathlib import Path
import shutil
import re
import json

# ========= НАСТРОЙКИ =========
ROOT_DIR = Path(r"N:\kad_arbitr\2024")   # <— укажите вашу директорию
DRY_RUN = False                              # True — только показать, что будет сделано

# Разделители между полями в имени файла: « — » (emdash) или " - "
SEP_RE = re.compile(r"\s+—\s+|\s+-\s+", flags=re.UNICODE)

# Регексы для номера дела (поддерживаются: А11-1642_2024, А40-12345/2020, СИП-715/2023 и т.п.)
CASE_RE = re.compile(
    r"(?:[АA]\d{1,3}-\d{1,7}[/_]\d{4}|СИП-\d{1,7}(?:[-/]\d{4})?)",
    flags=re.IGNORECASE | re.UNICODE
)

def safe_dirname(name: str) -> str:
    """Санитизация имени папки под Windows/*nix."""
    name = re.sub(r'[<>:"/\\|?*\x00-\x1F]', " ", name)
    name = re.sub(r"\s+", " ", name).strip().strip(".")
    return name or "UNKNOWN_COURT"

def normalize_case_no(s: str) -> str:
    """Нормализация номера для сопоставления (без пробелов, /->_, лат/кир 'А' унифицируем)."""
    if not s:
        return ""
    s = s.strip()
    # унифицируем варианты разделителей года
    s = s.replace("\\", "/").replace("_", "/")
    # убираем пробелы
    s = re.sub(r"\s+", "", s)
    # приводим кир/лат A к одному виду
    trans = str.maketrans({"а": "A", "А": "A", "a": "A", "A": "A"})
    s = s.translate(trans)
    s = s.upper()
    # финальный ключ — через "_"
    return s.replace("/", "_")

def parse_filename(fname: str):
    """
    Возвращает (case_no, court) из имени файла.
    Ожидается минимум: НОМЕР — СУД — ...
    """
    name = Path(fname).stem  # без расширения (чтобы не мешал индекс)
    parts = SEP_RE.split(name)
    if len(parts) < 2:
        return None, None
    case_no = parts[0].strip()
    court = parts[1].strip()
    # в случае если номер не похож — попробуем выдернуть регексом
    if not CASE_RE.search(case_no):
        m = CASE_RE.search(name)
        if m:
            case_no = m.group(0)
    return case_no, court

def index_json_by_case(root: Path):
    """
    Индексирует json-файлы по номеру дела:
    - ключ = normalize_case_no(номер)
    - номер берём из имени файла или из содержимого.
    Возвращает dict: key -> set(Path)
    """
    idx = {}
    for p in root.glob("*.json"):
        keys = set()

        # 1) По имени файла
        name_key = normalize_case_no(p.stem)
        if CASE_RE.search(p.stem):
            keys.add(name_key)

        # 2) По содержимому
        try:
            text = p.read_text(encoding="utf-8", errors="ignore").strip()
            # Попытаемся сначала как json:
            case_candidates = set()
            try:
                obj = json.loads(text)
                if isinstance(obj, str):
                    # файл содержит просто строку-номер
                    if CASE_RE.search(obj):
                        case_candidates.add(normalize_case_no(obj))
                elif isinstance(obj, dict):
                    # попробуем типичные поля
                    for k in ("case", "case_no", "case_number", "номер", "номер_дела"):
                        if k in obj and isinstance(obj[k], str) and CASE_RE.search(obj[k]):
                            case_candidates.add(normalize_case_no(obj[k]))
                    # если не нашли — выдернем любой номер из сериализованного текста
                    for m in CASE_RE.findall(text):
                        case_candidates.add(normalize_case_no(m))
                else:
                    # массив/прочее — ищем регексом по тексту
                    for m in CASE_RE.findall(text):
                        case_candidates.add(normalize_case_no(m))
            except json.JSONDecodeError:
                # не JSON — ищем регексом как по тексту
                for m in CASE_RE.findall(text):
                    case_candidates.add(normalize_case_no(m))

            keys |= case_candidates
        except Exception:
            pass

        # Если вообще не нашли номер — попробуем стем как fallback
        if not keys:
            keys.add(name_key)

        for k in keys:
            idx.setdefault(k, set()).add(p)

    return idx

def move_with_collision_avoid(src: Path, dst_dir: Path):
    """Перенос файла с избеганием коллизий имён."""
    dst_dir.mkdir(parents=True, exist_ok=True)
    target = dst_dir / src.name
    if target.exists():
        stem, suf = target.stem, target.suffix
        i = 2
        while True:
            candidate = dst_dir / f"{stem} ({i}){suf}"
            if not candidate.exists():
                target = candidate
                break
            i += 1
    if DRY_RUN:
        print(f"[DRY] MOVE: {src}  ->  {target}")
    else:
        shutil.move(str(src), str(target))
    return target

def main():
    root = ROOT_DIR
    assert root.exists() and root.is_dir(), f"Путь не найден или не папка: {root}"

    # Индексируем JSON по номеру дела один раз
    json_index = index_json_by_case(root)

    moved_files = 0
    moved_jsons = 0
    skipped = []

    for p in root.iterdir():
        if p.is_dir():
            continue
        if p.suffix.lower() == ".json":
            # json переносим только вместе с делом, на этом этапе пропускаем
            continue

        case_no, court = parse_filename(p.name)
        if not case_no or not court or not CASE_RE.search(case_no):
            skipped.append(p.name)
            continue

        court_dir = root / safe_dirname(court)
        # переносим основной файл
        new_path = move_with_collision_avoid(p, court_dir)
        moved_files += 1

        # подхватываем json по номеру
        key = normalize_case_no(case_no)
        jsons = list(json_index.get(key, []))
        for jp in jsons:
            if not jp.exists() or jp.parent != root:
                continue  # уже перенесён/удалён
            move_with_collision_avoid(jp, court_dir)
            moved_jsons += 1
            # чтобы не переносить повторно тем же ключом
            json_index[key].discard(jp)

    # Отчёт
    print("\n=== ГОТОВО ===")
    print(f"Перенесено файлов (не json): {moved_files}")
    print(f"Подтянуто JSON:              {moved_jsons}")
    if skipped:
        print("\nНе удалось распарсить (пропущены):")
        for name in skipped:
            print(" -", name)

if __name__ == "__main__":
    main()
