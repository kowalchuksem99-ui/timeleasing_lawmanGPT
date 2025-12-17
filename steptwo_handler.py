#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Сборка дел из PDF без OCR:
- читает все .pdf из SRC_DIR,
- извлекает текст (PyMuPDF, только текстовый слой),
- парсит метаданные из имени файла,
- формирует шапку,
- объединяет все файлы с одинаковым номером дела в один .txt в OUT_DIR.

Требуется: pip install pymupdf
"""

from __future__ import annotations

import os
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict

import fitz  # PyMuPDF


# ==================== НАСТРОЙКИ ====================

SRC_DIR = r"C:\Users\User\Desktop\sorted_pdf"         # <- Папка с PDF
OUT_DIR = r"C:\Users\User\Desktop\text_txt"          # <- Куда класть собранные TXT

# Кодировка сохранения итоговых файлов
OUT_ENCODING = "utf-8"

# Регулярка номера дела:
#   - поддерживает кириллическую А и латинскую A
#   - разделитель перед годом: -, / или _
#   - СИП-... с годом или без
CASE_RE = re.compile(
    r"(?:(?:[АA]\d{1,3}-\d{1,7}[-/_]\d{4})|(?:СИП-\d{1,7}(?:[-/_]\d{4})?))",
    re.IGNORECASE,
)

# Сплитер сегментов имени: " — " (em/en/ascii dash c пробелами)
SEGMENT_SPLIT_RE = re.compile(r"\s+[—–-]\s+")

# Значения, которые считаем «пусты ми»
EMPTY_TOKENS = {
    "", "н/д", "не указано", "нет данных",
    "суд не извлечён", "суд_не_извлечён",
}


# ==================== МОДЕЛИ ====================

@dataclass
class FileMeta:
    case_id: str | None
    court: str | None
    plaintiff: str | None
    defendants: List[str]
    filename: str
    text: str


@dataclass
class CaseBucket:
    case_id: str
    files: List[FileMeta] = field(default_factory=list)

    def merge_court(self) -> str | None:
        # первый осмысленный
        for fm in self.files:
            if fm.court and fm.court.lower() not in EMPTY_TOKENS:
                return fm.court
        return None

    def merge_plaintiff(self) -> str | None:
        for fm in self.files:
            if fm.plaintiff and fm.plaintiff.lower() not in EMPTY_TOKENS:
                return fm.plaintiff
        return None

    def merge_defendants(self) -> List[str]:
        bag: List[str] = []
        seen = set()
        for fm in self.files:
            for d in fm.defendants:
                key = d.lower()
                if key and key not in seen and key not in EMPTY_TOKENS:
                    seen.add(key)
                    bag.append(d)
        return bag


# ==================== УТИЛИТЫ ====================

def _norm_spaces(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip(" ;,")
    return s


def _cleanup_entity(s: str | None) -> str | None:
    if not s:
        return None
    # убираем подчёркивания как «псевдо-курсив» из имён (ООО _НТС-РЕСУРС_)
    s = s.replace("_", " ")
    s = _norm_spaces(s)
    return s or None


def _normalize_case(case_raw: str) -> str:
    """
    Приводим номер дела к виду:
      А07-243/2020  (подчёркивание => /)
    Сохраняем регистр буквы 'А' как в оригинале (кирилл/латин не меняем).
    """
    s = case_raw.strip()
    # только в номере дела '_' трактуем как '/'
    s = s.replace("_", "/")
    # двойные разделители в единичные
    s = re.sub(r"[-/_]+", lambda m: m.group(0)[0], s)
    return s


def parse_filename(stem: str) -> tuple[str | None, str | None, str | None, List[str]]:
    """
    Ожидаемый формат:
        <case> — <court> — <plaintiff> — <defendant(s)> — ...
    где ответчики могут быть перечислены через ';'
    """
    parts = SEGMENT_SPLIT_RE.split(stem)
    parts = [p.strip() for p in parts if p is not None]

    case_id = None
    court = plaintiff = None
    defendants: List[str] = []

    # 1) НОМЕР ДЕЛА — пытаемся вынуть из 1-го сегмента
    if parts:
        m = CASE_RE.search(parts[0])
        if m:
            case_id = _normalize_case(m.group(0))

    # 2) СУД
    if len(parts) >= 2:
        court = _cleanup_entity(parts[1])

    # 3) ИСТЕЦ
    if len(parts) >= 3:
        plaintiff = _cleanup_entity(parts[2])

    # 4) ОТВЕТЧИК(И)
    if len(parts) >= 4:
        # делим по ';'
        raw = parts[3]
        defs = [ _cleanup_entity(x) for x in raw.split(";") ]
        defendants = [d for d in defs if d]

    return case_id, court, plaintiff, defendants


def extract_pdf_text(pdf_path: Path) -> str:
    """
    Извлекает текст БЕЗ OCR (только текстовый слой).
    Если у PDF нет текста (скан), вернёт пустую строку.
    """
    out_chunks: List[str] = []
    try:
        with fitz.open(pdf_path) as doc:
            for page in doc:
                # стандартный потоковый «простой» текст
                out_chunks.append(page.get_text("text"))
    except Exception as exc:
        print(f"⚠ Ошибка чтения {pdf_path.name}: {exc}")
        return ""
    text = "\n".join(out_chunks)
    # Приводим пробелы, убираем хвостовые пустые строки
    text = text.replace("\r", "")
    text = re.sub(r"[ \t]+\n", "\n", text)
    return text.strip()


def build_header(case_id: str | None, court: str | None,
                 plaintiff: str | None, defendants: List[str]) -> str:
    dlist = "; ".join(defendants) if defendants else "N/A"
    lines = [
        f"Номер дела: {case_id or 'N/A'}",
        f"Суд: {court or 'N/A'}",
        f"Истец: {plaintiff or 'N/A'}",
        f"Ответчик: {dlist}",
    ]
    content = "\n".join(lines)
    border = "=" * 80
    title  = " ШАПКА ДЕЛА "
    # центрируем заголовок в пределах ширины границы
    pad = max(0, (len(border) - len(title)) // 2)
    title_line = f"{'=' * pad}{title}{'=' * (len(border) - len(title) - pad)}"
    return f"{title_line}\n{content}\n{border}\n\n"



def ensure_out_dir() -> Path:
    out = Path(OUT_DIR)
    out.mkdir(parents=True, exist_ok=True)
    (out / "unknown").mkdir(parents=True, exist_ok=True)
    return out


def natural_key(s: str):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]

def safe_stem(name: str) -> str:
    """Делаем безопасное имя для файла на Windows/macOS/Linux."""
    # заменяем запрещённые символы на дефис
    s = re.sub(r'[\\/:*?"<>|]+', '-', name)
    # убираем управляющие и приводим пробелы
    s = re.sub(r'\s+', ' ', s).strip()
    # на всякий случай ограничим длину (NTFS лимит ~255 байт)
    return s[:200]


# ==================== ОСНОВНОЙ ПРОЦЕСС ====================

def STEP_TWO():
    src = Path(SRC_DIR)
    out_dir = ensure_out_dir()

    if not src.exists():
        print(f"❌ Папка не найдена: {src}")
        sys.exit(1)

    # Группируем по делу
    buckets: Dict[str, CaseBucket] = {}
    singles: List[FileMeta] = []   # файлы без распознанного номера дела

    pdf_files = sorted(
        (p for p in src.glob("*.pdf")),
        key=lambda p: natural_key(p.name)
    )

    if not pdf_files:
        print(f"ℹ️  В {SRC_DIR} нет .pdf")
        return

    for pdf in pdf_files:
        stem = pdf.stem  # имя файла без .pdf
        case_id, court, plaintiff, defendants = parse_filename(stem)

        text = extract_pdf_text(pdf)
        if not text:
            print(f"⚠ В {pdf.name} не найден текстовый слой (возможно скан). Пропущен.")
            continue

        meta = FileMeta(
            case_id=case_id,
            court=court if (court and court.lower() not in EMPTY_TOKENS) else None,
            plaintiff=plaintiff if (plaintiff and plaintiff.lower() not in EMPTY_TOKENS) else None,
            defendants=[d for d in defendants if d and d.lower() not in EMPTY_TOKENS],
            filename=pdf.name,
            text=text
        )

        if meta.case_id:
            b = buckets.setdefault(meta.case_id, CaseBucket(case_id=meta.case_id))
            b.files.append(meta)
        else:
            singles.append(meta)

    # Пишем объединённые файлы по делам
    for case_id, bucket in buckets.items():
        bucket.files.sort(key=lambda fm: natural_key(fm.filename))
        court = bucket.merge_court()
        plaintiff = bucket.merge_plaintiff()
        defendants = bucket.merge_defendants()

        pieces: List[str] = []
        # Общая шапка по делу (один раз)
        pieces.append(build_header(case_id, court, plaintiff, defendants))

        # Просто склеиваем тексты всех PDF без подписи имени файла
        for fm in bucket.files:
            pieces.append(fm.text.strip())
            pieces.append("\n\n")  # разделяем пустой строкой

        content = "\n".join(pieces).strip() + "\n"
        out_path = out_dir / f"{safe_stem(case_id)}.txt"
        out_path.write_text(content, encoding=OUT_ENCODING)
        print(f"✔ Собрано дело: {out_path.name}  ({len(bucket.files)} PDF)")

    # Файлы без номера дела — сохраняем по одному в unknown/
    for fm in singles:
        header = build_header(None, fm.court, fm.plaintiff, fm.defendants)
        content = header + fm.text + "\n"
        out_path = out_dir / "unknown" / (safe_stem(Path(fm.filename).stem) + ".txt")
        out_path.write_text(content, encoding=OUT_ENCODING)
        print(f"✔ Сохранён без номера: unknown/{out_path.name}")

    print("🎉 Готово.")



STEP_TWO()