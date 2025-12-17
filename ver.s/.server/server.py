from __future__ import annotations
import re
import os
import httpx
import textwrap
from typing import List
import traceback
import logging

from flask import Flask, jsonify, request
from flask_cors import CORS
from openai import OpenAI
from qdrant_client import QdrantClient, models
import tiktoken
# в начале файла
logging.basicConfig(level=logging.DEBUG)
# ─────────────────── конфигурация ───────────────────
API_KEY = ("API KEY")
COLLECTION = "kad_cases"
EMB_MODEL = "EMBED MODEL"
GPT5_MODEL = "gpt-4.1"
# Убедитесь, что это совпадает с векторным размером в Qdrant
DIM = 768
TOP_K = 5
# Регэксп для поиска номера дела
CASE_RE = re.compile(r"[AB]\d{1,3}-\d{3,6}[/-]\d{4}", re.I)

# ───── прокси через VPN (SOCKS5) ─────
PROXY_URL = "socks5://127.0.0.1:5000"
os.environ["HTTP_PROXY"]  = PROXY_URL
os.environ["HTTPS_PROXY"] = PROXY_URL
os.environ["ALL_PROXY"]   = PROXY_URL


# init clients
http_client = httpx.Client(proxy=PROXY_URL, timeout=120)
openai_client = OpenAI(api_key=API_KEY, http_client=http_client)
qdrant = QdrantClient(
    host="127.0.0.1",
    port=6333,
    https=False,
    api_key="API KEY",
    prefer_grpc=False,   # явно выключаем gRPC, используем HTTP(S)
    timeout=600.0
)

app = Flask(__name__)
CORS(app)


# ─────────────────── вспомогательные функции ───────────────────
def _embed(text: str) -> List[float]:
    """Получить эмбеддинг из OpenAI."""
    resp = openai_client.embeddings.create(
        model=EMB_MODEL,
        input=text,
        dimensions=DIM
    )
    return resp.data[0].embedding

def _normalize_case_id(s: str) -> str:
    # приводим тире к обычному, делаем верхний регистр и латиницу A/B
    s = s.replace("—", "-").replace("–", "-").replace("−", "-").upper()
    return s.translate(str.maketrans({"А": "A", "В": "B"}))

def _fetch_all_case_chunks(case_num: str) -> List[str]:
    """Забирает ВСЕ чанки с данным case_id по фильтру через scroll."""
    filt = models.Filter(
        must=[models.FieldCondition(key="case_id",
                                    match=models.MatchValue(value=case_num))]
    )
    all_chunks: List[str] = []
    next_off = None
    while True:
        points, next_off = qdrant.scroll(
            collection_name=COLLECTION,
            limit=256,                 # можно 512
            with_payload=True,
            with_vectors=False,
            filter=filt,
            offset=next_off,
        )
        if not points:
            break
        all_chunks.extend(p.payload.get("text", "") for p in points)
        if next_off is None:
            break
    return all_chunks

def _ask(question: str, max_tokens: int = 250) -> str:
    MAX_CHUNKS = 800
    MAX_CONTEXT_CHARS = 150_000
    MAX_PROMPT_CHARS = 180_000

    want_all = re.search(r"\b(все|всё|полностью|полное|целиком)\b", question, re.IGNORECASE)
    m = CASE_RE.search(question)

    chunks = []
    case_num = None

    # --- 1. Поиск номера дела ---
    if m:
        case_num = _normalize_case_id(m.group(0))
        qdrant_filter = {"must": [{"key": "case_id", "match": {"value": case_num}}]}

        if want_all:
            # --- 2. Получение всех чанков, но не более MAX_CHUNKS ---
            all_chunks = []
            next_off = None
            real_count = 0

            while True:
                pts, next_off = qdrant.scroll(
                    collection_name=COLLECTION,
                    limit=256,
                    with_payload=True,
                    with_vectors=False,
                    filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key="case_id",
                                match=models.MatchValue(value=case_num)
                            )
                        ]
                    ),
                    offset=next_off,
                )

                if not pts:
                    break

                for p in pts:
                    real_count += 1
                    if len(all_chunks) < MAX_CHUNKS:
                        all_chunks.append(p.payload.get("text", ""))

                if next_off is None:
                    break

            if not all_chunks:
                return f"По делу {case_num} сведений в базе нет."

            chunks = all_chunks

        else:
            # --- 3. Обычный векторный поиск по делу ---
            query_text = f"<CASE:{case_num}> {question}"
            vec = _embed(query_text)
            hits = qdrant.search(
                collection_name=COLLECTION,
                query_vector=vec,
                limit=TOP_K,
                with_payload=True,
                query_filter=qdrant_filter,
            )
            chunks = [h.payload.get("text", "") for h in hits]

    else:
        # --- 4. Поиск без номера дела ---
        vec = _embed(question)
        hits = qdrant.search(
            collection_name=COLLECTION,
            query_vector=vec,
            limit=TOP_K,
            with_payload=True,
        )
        chunks = [h.payload.get("text", "") for h in hits]

    # --- 5. Ограничиваем контекст ---
    if not chunks:
        if case_num:
            return f"По делу {case_num} сведений в базе нет."
        else:
            return "По запросу подходящих фрагментов не найдено."

    # Ограничение количества чанков
    chunks = chunks[:MAX_CHUNKS]

    # Ограничение текста
    context = "\n\n---\n\n".join(chunks)
    if len(context) > MAX_CONTEXT_CHARS:
        context = context[:MAX_CONTEXT_CHARS]

    # --- 6. Собираем промпт ---
    prompt = textwrap.dedent(f"""
    Ты — юридический ассистент по РФ. Работай строго по правилам ниже.
    Никаких преамбул, рассуждений и ссылок на инструкции — выводи только результат.

    Режимы:
    A) Конкретное дело — если в вопросе указан номер дела (А/В-…/год, СИП-…, и др.).
    B) Подборка дел — если просят найти/подобрать дела/обзор практики по теме.
    C) Общий юр-вопрос — любые юридические вопросы без конкретного дела.

    Правила:
    • В режимах A/B используй ТОЛЬКО данные из <RAG>…</RAG>.
    • Если фрагментов слишком много или они усечены — допиши в «Особенность»: «анализ на основе сокращённого набора фрагментов».
    • Шаблон записи дела:
      **Номер дела:**
      **Истец:**
      **Ответчик:**
      **Суд:**
      **Стадия:**
      **Частности дела:**
      **Краткий синопсис:**
      **Особенность:**

    • Между делами:
      ──────────────────────────────────────────────────────────────────

    • Если данных нет: 
      A — «По предоставленным фрагментам сведений по делу не найдено.»
      B — «По запросу подходящих дел в базе не найдено.»

    Режим C (общий вопрос):
    1) Краткий вывод.
    2) Правовое обоснование.
    3) Исключения и риски.
    4) Практические шаги.
    Если вопрос не юридический — ответь дословно:
    "Извините, но я не могу отвечать на вопросы по отвлеченным тематикам, давайте лучше поговорим в рамках юридического поля?"

    <RAG>
    {context}
    </RAG>

    <QUESTION>
    {question}
    </QUESTION>
    """)

    # Ограничиваем итоговый промпт
    if len(prompt) > MAX_PROMPT_CHARS:
        prompt = prompt[:MAX_PROMPT_CHARS]

    # --- 7. GPT запрос ---
    rsp = openai_client.responses.create(
        model=GPT5_MODEL,
        input=prompt,
        max_output_tokens=max_tokens
    )

    text = getattr(rsp, "output_text", None)
    return text.strip() if text else "[Пустой ответ]"


# ─────────────────── Flask-эндпоинт ───────────────────
@app.route("/chat", methods=["POST"])
def chat():
    data = request.get_json(force=True)
    msgs = data.get("messages")
    if not msgs:
        return jsonify({"error": "'messages' array required"}), 400

    user_msg = next((m for m in reversed(msgs) if m.get("role") == "user"), None)
    if not user_msg:
        return jsonify({"error": "no user message"}), 400

    try:
        limit = int(data.get("max_tokens", 5_00))
        # жёстко ограничим диапазон, чтобы избежать злоупотреблений
        limit = max(250, min(limit, 16_000))
        answer = _ask(user_msg["content"], max_tokens=limit)
        return jsonify({"answer": answer})
    except Exception as exc:
        # залогировать подробности на консоль
        logging.exception("Error in /chat")
        # вернуть стек в тело ответа, чтобы клиент увидел, что упало
        return jsonify({
            "error": str(exc),
            "trace": traceback.format_exc().splitlines()
        }), 500


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5005)
