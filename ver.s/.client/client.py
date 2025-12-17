from __future__ import annotations

import json
import os
import re
import sys
import threading
from html import unescape
from typing import Any, Dict, List

import requests
import markdown
from PySide6.QtCore import Qt, QTimer, Signal, QObject, QDateTime
from datetime import datetime
from PySide6.QtGui import QFont, QGuiApplication
from PySide6.QtWidgets import (
    QApplication,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSplitter,
    QTextEdit,
    QVBoxLayout,
    QWidget,
)
from PySide6.QtCore import (QPropertyAnimation, QEasingCurve, QPointF, QParallelAnimationGroup,
                            QSequentialAnimationGroup,
                            QPauseAnimation)

SERVER_URL = "http://127.0.0.1:5000/chat"
SAVE_PATH = os.path.join(os.path.expanduser("~"), ".tlawman_chats.json")

TOKENS_MIN = 5_000  # минимальный лимит, «короткий» ответ
TOKENS_MAX = 15_000  # максимальный лимит, «длинный» ответ


# ─────────────────── вспомогательные сигналы ────────────────────
class WorkerSignals(QObject):
    finished = Signal(str)
    error = Signal(str)


# ───────────────── CopyableLabel ─────────────────
class CopyableLabel(QLabel):
    """QLabel, который позволяет выделять и копировать текст из сообщений"""

    def __init__(self, html: str, max_width: int):
        super().__init__(html)
        self.setTextFormat(Qt.RichText)
        self.setWordWrap(True)
        self.setMaximumWidth(max_width)
        # Разрешаем тексту быть выделенным и скопированным
        self.setTextInteractionFlags(Qt.TextSelectableByMouse | Qt.TextSelectableByKeyboard)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_context_menu)

    def _show_context_menu(self, pos):
        menu = QMenu(self)
        copy_action = menu.addAction("Копировать")
        action = menu.exec(self.mapToGlobal(pos))

        if action is copy_action:
            clipboard = QGuiApplication.clipboard()

            if self.hasSelectedText():
                clipboard.setText(self.selectedText())
            else:
                clipboard.setText(self._html_to_text(self.text()))

    def _html_to_text(html: str) -> str:
        s = html
        # переносы строк
        s = re.sub(r'(?i)<br\s*/?>', '\n', s)
        s = re.sub(r'(?i)</p\s*>', '\n\n', s)
        # списки
        s = re.sub(r'(?i)<li[^>]*>', '• ', s)
        s = re.sub(r'(?i)</li\s*>', '\n', s)
        s = re.sub(r'(?i)</(ul|ol)\s*>', '\n', s)
        # заголовки
        s = re.sub(r'(?i)<h[1-6][^>]*>', '', s)
        s = re.sub(r'(?i)</h[1-6]\s*>', '\n\n', s)
        # убрать прочие теги
        s = re.sub(r'<[^>]+>', '', s)
        return unescape(s).strip()


# ───────────────────── «пузырь» сообщения ──────────────────────
class MessageBubble(QWidget):
    def __init__(self, sender: str, text: str, user: bool = False, max_width: int = 420):
        super().__init__()
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)

        # 1) Заменяем ***Заголовок*** на ## Заголовок
        md = re.sub(r'\*{3}(.*?)\*{3}', r'## \1', text)
        # md = re.sub(r'(?m)^\s*-\s+', '', md)

        # 2) Вставляем перевод строки перед каждым **что-либо**
        #    (?m) — многострочный режим, ^ и $ — начало/конец строки
        md = re.sub(r'(?m)^(?P<stars>\*\*(?!\s).*?\*\*)', r'\n\n\g<stars>', md)

        # 3) Добавляем в начало «Вы» или «TLawman» как жирный текст
        md = f"**{sender}:**\n\n{md}"

        LIST_INDENT_PX = 1  # ← задаёт длину отступа слева до текста

        html_body = markdown.markdown(md, extensions=["extra", "sane_lists", "nl2br"])

        # Стиль списков: маркёр снаружи, фиксируем отступ
        ul_style = f"margin:0.3em 0; padding-left:{LIST_INDENT_PX}px; list-style-position:outside;"
        ol_style = ul_style

        html_body = (html_body
                     .replace("<ul>", f"<ul style='{ul_style}'>")
                     .replace("<ol>", f"<ol style='{ol_style}'>")
                     .replace("<li>", "<li style='margin:0.15em 0;'>"))

        # 5) Рендерим в QLabel
        lbl = CopyableLabel(html_body, max_width)
        lbl.setTextFormat(Qt.RichText)
        lbl.setFont(QFont("Segoe UI", 11))
        lbl.setContentsMargins(10, 8, 10, 8)

        # Стили «пузыря»
        base_css = "border-radius:12px;"
        if user:
            lbl.setStyleSheet(
                base_css +
                "word-break:break-word;"
                "background:rgba(16,163,127,.6); color:#fff;"
                "border:1px solid rgba(16,163,127,.8);"
            )
            lay.addStretch()
            lay.addWidget(lbl)
        else:
            lbl.setStyleSheet(
                base_css +
                "background:rgba(226,232,240,.6);"
                "color:#1F2937;"
                "border:1px solid rgba(200,210,220,.8);"
            )
            lay.addWidget(lbl)
            lay.addStretch()


class TypingDots(QWidget):
    def __init__(self, color="#aaa"):
        super().__init__()
        dot = 6  # диаметр точки
        gap = 8
        jump = 7
        dur = 600
        delay = 180

        self.setFixedSize(dot * 3 + gap * 2, dot + jump)  # высоты хватает на прыжок
        grp = QParallelAnimationGroup(self)

        for i in range(3):
            x = i * (dot + gap)
            y0 = jump  # базовая позиция (внизу)
            ball = QLabel(self)
            ball.setFixedSize(dot, dot)
            ball.setStyleSheet(f"background:{color};border-radius:{dot // 2}px;")
            ball.move(x, y0)

            anim = QPropertyAnimation(ball, b"pos", self)
            anim.setDuration(dur)
            # вниз-вверх-вниз: y0 → 0 → y0
            anim.setKeyValueAt(0.0, QPointF(x, y0))
            anim.setKeyValueAt(0.5, QPointF(x, 0))
            anim.setKeyValueAt(1.0, QPointF(x, y0))
            anim.setEasingCurve(QEasingCurve.InOutSine)

            seq = QSequentialAnimationGroup(self)
            seq.addPause(i * delay)
            seq.addAnimation(anim)
            seq.setLoopCount(-1)
            grp.addAnimation(seq)

        grp.start()
        self._group = grp  # держим ссылку, чтобы GC не убрал


# ───────────────────────── главное окно ─────────────────────────
class InputTextEdit(QTextEdit):
    send_signal = Signal()

    def keyPressEvent(self, event):
        # Enter / Return ─ отправка
        if event.key() in (Qt.Key_Return, Qt.Key_Enter) and not (event.modifiers() & Qt.ShiftModifier):
            self.send_signal.emit()
        # Shift+Enter ─ просто перенос строки
        else:
            super().keyPressEvent(event)


class ChatGPTApp(QWidget):
    COLUMN_WIDTH = 700

    def __init__(self):
        super().__init__()
        self.setWindowTitle("TLawman")
        self.resize(1100, 700)
        self.setMinimumSize(800, 600)
        self.setStyleSheet("background:#2E2E2E;")

        self.dialogs: List[Dict[str, Any]] = []
        self.current_index: int = -1
        self.pending_widget: QWidget | None = None

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self._build_sidebar())
        splitter.addWidget(self._build_chat())
        splitter.setSizes([220, 880])

        root = QHBoxLayout(self)
        root.setContentsMargins(6, 6, 6, 6)
        root.addWidget(splitter)

        self.server_url = SERVER_URL

        self._connect_signals()
        self._load_dialogs_from_file()
        if not self.dialogs:
            self._new_chat()

    def _build_sidebar(self) -> QWidget:
        sb = QWidget()
        sb.setFixedWidth(220)
        sb.setAttribute(Qt.WA_StyledBackground, True)
        sb.setStyleSheet("background:#1E1E1E; border-radius:12px;")

        lay = QVBoxLayout(sb)
        lay.setContentsMargins(10, 15, 10, 15)
        lay.setSpacing(10)

        new_btn = QPushButton("＋  Новый чат")
        new_btn.setCursor(Qt.PointingHandCursor)
        new_btn.setStyleSheet(
            "QPushButton{background:#10A37F;color:#fff;font:14px 'Segoe UI';"
            "border:none;border-radius:8px;padding:6px 8px;}"
            "QPushButton:hover{background:#0c7c60;}"
        )
        new_btn.clicked.connect(self._new_chat)
        lay.addWidget(new_btn)

        self.chat_list = QListWidget()
        self.chat_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.chat_list.customContextMenuRequested.connect(self._on_chat_context_menu)
        self.chat_list.setStyleSheet(
            "QListWidget{background:#1E1E1E;color:#ccc;border:none;border-radius:8px;}"
            "QListWidget::item{height:28px;color:#ccc;}"
            "QListWidget::item:selected{background:#2E2E2E;color:#fff;border-radius:6px;}"
        )
        self.chat_list.currentRowChanged.connect(self._load_chat)
        self.chat_list.itemChanged.connect(self._on_title_changed)
        lay.addWidget(self.chat_list, 1)
        return sb

    def _on_chat_context_menu(self, pos):
        item = self.chat_list.itemAt(pos)
        if not item:
            return
        menu = QMenu(self.chat_list)
        rename, delete = menu.addAction("Переименовать"), menu.addAction("Удалить")
        act = menu.exec(self.chat_list.mapToGlobal(pos))
        row = self.chat_list.row(item)
        if act == rename:
            self.chat_list.editItem(item)
        elif act == delete:
            self._save_dialogs()
            del self.dialogs[row]
            self.chat_list.takeItem(row)
            if self.chat_list.count():
                self.chat_list.setCurrentRow(min(row, self.chat_list.count() - 1))
            else:
                self._new_chat()

    def _show_typing(self):
        if self.pending_widget:
            return
        self.pending_widget = TypingDots()
        self.chat_layout.insertWidget(self.chat_layout.count() - 1,
                                      self.pending_widget)
        QTimer.singleShot(0, self._scroll_bottom)

    def _hide_typing(self):
        if self.pending_widget:
            self.pending_widget.setParent(None)
            self.pending_widget = None

    def _build_chat(self) -> QWidget:
        center = QWidget()
        center.setAttribute(Qt.WA_StyledBackground, True)
        center.setStyleSheet("background:#2E2E2E;border-radius:12px;")
        v = QVBoxLayout(center)
        v.setContentsMargins(15, 15, 15, 15)
        v.setSpacing(0)

        self.chat_container = QWidget()
        self.chat_container.setStyleSheet("background:transparent;")
        self.chat_layout = QVBoxLayout(self.chat_container)
        self.chat_layout.setContentsMargins(15, 15, 15, 10)
        self.chat_layout.setSpacing(10)
        self.chat_layout.addStretch()

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.chat_container)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.scroll_area.setStyleSheet(
            "QScrollBar:vertical{background:transparent;width:8px;}"
            "QScrollBar::handle:vertical{background:rgba(200,200,200,0.6);min-height:20px;border-radius:4px;}"
            "QScrollBar::add-line:vertical,QScrollBar::sub-line:vertical{height:0px;}"
            "QScrollBar::add-page, QScrollBar::sub-page{background:none;}"
        )
        bar = self.scroll_area.verticalScrollBar()
        bar.rangeChanged.connect(lambda _min, _max: bar.setValue(_max))
        v.addWidget(self.scroll_area, 1)

        h = QHBoxLayout()
        h.setSpacing(15)
        v.addLayout(h)
        self.input_container = QWidget()
        self.input_container.setStyleSheet("border:2px solid #ccc;border-radius:14px;background:#fff;")
        ic = QHBoxLayout(self.input_container)
        ic.setContentsMargins(5, 5, 5, 5)
        self.input_line = InputTextEdit()
        self.input_line.setFixedHeight(30)
        self.input_line.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.input_line.setPlaceholderText("Введите сообщение…")
        self.input_line.setStyleSheet("border:none;background:transparent;font:15px 'Segoe UI';")
        self.input_line.textChanged.connect(self._adjust_input_height)
        self.input_line.send_signal.connect(self._send_message)
        ic.addWidget(self.input_line)
        h.addWidget(self.input_container, 1)
        # ── переключатель токен-лимита ──────────────────────────
        self.token_limit = TOKENS_MIN  # стартовое значение
        self.token_btn = QPushButton(str(TOKENS_MIN))
        self.token_btn.setFixedSize(60, 45)
        self.token_btn.setCursor(Qt.PointingHandCursor)
        self.token_btn.setStyleSheet(
            "background:#4A5568;color:#fff;font:14px 'Segoe UI';"
            "border:none;border-radius:22px;")
        self.token_btn.clicked.connect(self._toggle_tokens)
        h.addWidget(self.token_btn)

        def btn(txt, bg):
            b = QPushButton(txt)
            b.setFixedSize(45, 45)
            b.setCursor(Qt.PointingHandCursor)
            b.setStyleSheet(f"background:{bg};color:#fff;font:20px 'Segoe UI';border:none;border-radius:22px;")
            return b

        self.send_btn = btn("➤", "#10A37F")
        self.send_btn.clicked.connect(self._send_message)
        self.clear_btn = btn("🗑", "#4A5568")
        self.clear_btn.clicked.connect(self._clear_chat_ui)
        h.addWidget(self.send_btn)
        h.addWidget(self.clear_btn)
        return center

    def _toggle_tokens(self):
        self.token_limit = (
            TOKENS_MAX if self.token_limit == TOKENS_MIN else TOKENS_MIN
        )
        self.token_btn.setText(str(self.token_limit))

    def _adjust_input_height(self):
        doc_h = self.input_line.document().size().height()
        new_h = max(30, min(250, int(doc_h)))
        if self.input_line.height() != new_h:
            self.input_line.setFixedHeight(new_h)
            self.input_container.setFixedHeight(new_h + 8)

    def _scroll_bottom(self):
        bar = self.scroll_area.verticalScrollBar()
        bar.setValue(bar.maximum())

    def _new_chat(self):
        self.dialogs.append({"title": f"Чат {len(self.dialogs) + 1}", "messages": []})
        item = QListWidgetItem(self.dialogs[-1]["title"])
        item.setFlags(item.flags() | Qt.ItemIsEditable)
        self.chat_list.addItem(item)
        self.chat_list.setCurrentItem(item)
        self._save_dialogs()

    def _load_chat(self, row):
        if row < 0 or row >= len(self.dialogs):
            return
        self.current_index = row

        # очистка старых виджетов
        for i in reversed(range(self.chat_layout.count() - 1)):
            w = self.chat_layout.itemAt(i).widget()
            if w:
                w.setParent(None)

        # восстанавливаем все сообщения
        for rec in self.dialogs[row]["messages"]:
            if len(rec) == 4:
                snd, txt, usr, ts = rec
            else:
                snd, txt, usr = rec
                ts = None
            self._create_bubble(snd, txt, usr, ts)  # ← теперь внутри цикла

        QTimer.singleShot(0, self._scroll_bottom)

    def _create_bubble(self, sender, text, user, ts: str | None = None):
        if ts:
            dt = QDateTime.fromString(ts, Qt.ISODate)
        else:
            dt = QDateTime.currentDateTime()
        lbl_time = QLabel(dt.toString("dd.MM.yyyy HH:mm"))
        lbl_time.setAlignment(Qt.AlignCenter)
        lbl_time.setStyleSheet("color:#888;font:10px 'Segoe UI';")
        self.chat_layout.insertWidget(self.chat_layout.count() - 1, lbl_time)
        bub = MessageBubble(
            sender, text, user,
            max_width=int(self.COLUMN_WIDTH * (0.6 if user else 0.8)))
        self.chat_layout.insertWidget(self.chat_layout.count() - 1, bub)

    def _append(self, sender, text, user):
        ts = datetime.now().isoformat(timespec="seconds")  # ▸ ISO-время

        self.dialogs[self.current_index]["messages"].append([sender, text, user, ts])
        self._create_bubble(sender, text, user, ts)
        QTimer.singleShot(0, self._scroll_bottom)
        self._save_dialogs()

    def _build_openai_messages(self, new_msg):
        msgs = []
        for rec in self.dialogs[self.current_index]["messages"]:
            snd, txt, usr = rec[:3]  # берём только первые три поля
            msgs.append({
                "role": "user" if usr else "assistant",
                "content": txt
            })
        msgs.append({"role": "user", "content": new_msg})
        return msgs

    def _send_message(self):
        msg = self.input_line.toPlainText().strip()
        if not msg:
            return
        self._append("Вы", msg, True)
        self.input_line.clear()

        self._show_typing()  # ← новая строка

        threading.Thread(target=self._ask_server,
                         args=(msg,), daemon=True).start()

    def _clear_chat_ui(self):
        if self.current_index < 0:
            return
        self._hide_typing()  # ← добавили
        self.dialogs[self.current_index]["messages"].clear()
        self._load_chat(self.current_index)
        self._save_dialogs()

    def _ask_server(self, prompt):
        try:
            payload = {
                "messages": self._build_openai_messages(prompt),
                "max_tokens": self.token_limit
            }
            rsp = requests.post(self.server_url, json=payload, timeout=600)
            rsp.raise_for_status()
            data = rsp.json()
            ans = data.get("answer", "[Пустой ответ]")
            self.signals.finished.emit(ans)
        except Exception as e:
            self.signals.error.emit(str(e))

    def _connect_signals(self):
        self.signals = WorkerSignals()
        self.signals.finished.connect(
            lambda txt: (self._hide_typing(),
                         self._append("TLawman", txt, False)))
        self.signals.error.connect(
            lambda err: (self._hide_typing(),
                         self._append("Ошибка", err, False)))

    def _save_dialogs(self):
        try:
            with open(SAVE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.dialogs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[Ошибка при сохранении]: {e}")

    def _on_title_changed(self, item):
        row = self.chat_list.row(item)
        if 0 <= row < len(self.dialogs):
            self.dialogs[row]["title"] = item.text()
            self._save_dialogs()

    def _load_dialogs_from_file(self):
        if not os.path.exists(SAVE_PATH): return
        try:
            with open(SAVE_PATH, "r", encoding="utf-8") as f:
                self.dialogs = json.load(f)
            for dlg in self.dialogs:
                item = QListWidgetItem(dlg["title"])
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.chat_list.addItem(item)
            if self.dialogs: self.chat_list.setCurrentRow(0)
        except Exception as e:
            print(f"[Ошибка при загрузке]: {e}");
            self.dialogs = []


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet(
        """
        QMenu { 
            background-color: rgba(30, 30, 30, 0.95); 
            color: #ccc; 
            border: none; 
            border-radius: 8px; 
            padding: 4px; 
        }
        QMenu::item { 
            padding: 6px 20px; 
            border-radius: 6px; 
        }
        QMenu::item:selected { 
            background-color: #10A37F; 
            color: #fff; 
        }
        QLineEdit { 
            color: #10A37F; 
            background: #2E2E2E; 
            border: 1px solid #10A37F;
            border-radius: 4px;
        }
        """
    )
    win = ChatGPTApp()
    win.show()
    sys.exit(app.exec())
