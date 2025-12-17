import sys
import threading
from typing import List, Dict, Any

from PySide6.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QSplitter,
    QListWidget,
    QListWidgetItem,
    QTextEdit,
    QPushButton,
    QLabel,
    QScrollArea,
    QSizePolicy,
    QMenu
)
from PySide6.QtCore import Qt, Signal, QObject, QTimer
from PySide6.QtGui import QFont
from openai import OpenAI
import json
import os

SAVE_PATH = os.path.join(os.path.expanduser("~"), ".tlawman_chats.json")


# ─────────────────── вспомогательные сигналы ────────────────────
class WorkerSignals(QObject):
    finished = Signal(str)
    error = Signal(str)


# ───────────────────── «пузырь» сообщения ──────────────────────
class MessageBubble(QWidget):

    def __init__(self, sender: str, text: str, user: bool = False, max_width: int = 420):
        super().__init__()
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        lay = QHBoxLayout(self)
        lay.setContentsMargins(0, 0, 0, 0)
        lay.setSpacing(0)

        lbl = QLabel(f"<b>{sender}:</b><br>{text}")
        lbl.setTextFormat(Qt.RichText)
        lbl.setWordWrap(True)
        lbl.setMaximumWidth(max_width)
        lbl.setFont(QFont("Segoe UI", 11))
        lbl.setContentsMargins(10, 8, 10, 8)

        base_css = "border-radius:12px;"
        if user:
            lbl.setStyleSheet(
                base_css
                + """
                word-break:break-word;
                background:rgba(16,163,127,.6); color:#fff;
                border:1px solid rgba(16,163,127,.8);
            """)
            lay.addStretch()
            lay.addWidget(lbl)
        else:
            lbl.setStyleSheet(
                base_css
                + """
                background:rgba(226,232,240,.6); 
                color:#1F2937;
                border:1px solid rgba(200,210,220,.8);
            """)
            lay.addWidget(lbl)
            lay.addStretch()


# ───────────────────────── главное окно ─────────────────────────

class InputTextEdit(QTextEdit):
    send_signal = Signal()

    def keyPressEvent(self, event):
        if event.modifiers() == Qt.ControlModifier and event.key() in (Qt.Key_Return, Qt.Key_Enter):
            self.send_signal.emit()
        else:
            super().keyPressEvent(event)


class ChatGPTApp(QWidget):
    COLUMN_WIDTH = 700

    def __init__(self):
        super().__init__()
        self.setWindowTitle("TLawman")
        self.resize(1100, 700)
        self.setMinimumSize(800, 600)
        self.setStyleSheet("""
                background:#2E2E2E;
            """)

        self.dialogs: List[Dict[str, Any]] = []
        self.current_index: int = -1

        splitter = QSplitter(Qt.Horizontal)
        splitter.addWidget(self._build_sidebar())
        splitter.addWidget(self._build_chat())
        splitter.setSizes([220, 880])

        root = QHBoxLayout(self)
        root.setContentsMargins(6, 6, 6, 6)
        root.addWidget(splitter)

        self.client = OpenAI(api_key="sk-proj-p6nYMbWSIjpbRwS8aFLOtg_dTQPO3H"
                                     "-NxbG4cQaFpuQMcgc97gBwvxLcMT5zMky_enlWT1UU0hT3BlbkFJpWCSljMwAbQxMqpvb2"
                                     "-RUrtUqOS1XNIrF8yQvwIM2j-G5arl4L6V"
                                     "-ss4MTeye-qi1-pHb0e1cA")
        self._connect_signals()
        self._load_dialogs_from_file()
        if not self.dialogs:
            self._new_chat()

    # ───────────────── sidebar ─────────────────
    def _build_sidebar(self) -> QWidget:
        sb = QWidget()
        sb.setFixedWidth(220)
        sb.setAttribute(Qt.WA_StyledBackground, True)
        sb.setStyleSheet("""
                 background:#1E1E1E;
                 border-radius:12px;
             """)

        lay = QVBoxLayout(sb)
        lay.setContentsMargins(10, 15, 10, 15)
        lay.setSpacing(10)

        new_btn = QPushButton("＋  Новый чат")
        new_btn.setCursor(Qt.PointingHandCursor)
        new_btn.setStyleSheet("""
            QPushButton{
                background:#10A37F;
                color:#fff;
                font:14px 'Segoe UI';
                border:none;
                border-radius:8px;
                padding:6px 8px;
            }
            QPushButton:hover{
                background:#0c7c60;
            }
            """)
        new_btn.clicked.connect(self._new_chat)
        lay.addWidget(new_btn)

        self.chat_list = QListWidget()
        self.chat_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.chat_list.customContextMenuRequested.connect(self._on_chat_context_menu)
        self.chat_list.setAttribute(Qt.WA_StyledBackground, True)
        self.chat_list.setStyleSheet("""
            QListWidget {
                background: #1E1E1E;
                color: #ccc;
                border: none;
                border-radius: 8px;
            }
            QListWidget::item {
                height: 28px;
                color: #ccc;
            }
            QListWidget::item:selected {
                background: #2E2E2E;
                color: #fff;
                border-radius: 6px;
            }
            QListWidget QLineEdit {
                color: #10A37F;
                background: #2E2E2E;
                border: 1px solid #10A37F;
                border-radius: 4px;
            }
        """)
        self.chat_list.currentRowChanged.connect(self._load_chat)
        self.chat_list.itemChanged.connect(self._on_title_changed)

        lay.addWidget(self.chat_list, 1)

        return sb

    def _on_chat_context_menu(self, pos):
        item = self.chat_list.itemAt(pos)
        if item is None:
            return

        menu = QMenu(self.chat_list)
        rename_action = menu.addAction("Переименовать")
        delete_action = menu.addAction("Удалить")
        action = menu.exec(self.chat_list.mapToGlobal(pos))

        row = self.chat_list.row(item)

        if action == rename_action:
            self.chat_list.editItem(item)

        elif action == delete_action:
            self._save_dialogs()
            del self.dialogs[row]
            self.chat_list.takeItem(row)
            if self.chat_list.count():
                new_row = min(row, self.chat_list.count() - 1)
                self.chat_list.setCurrentRow(new_row)
            else:
                self._new_chat()

    # ───────────────── chat pane ─────────────────
    def _build_chat(self) -> QWidget:
        center = QWidget()
        center.setAttribute(Qt.WA_StyledBackground, True)
        center.setStyleSheet("""
                background:#2E2E2E;
                border-radius:12px;
            """)
        v = QVBoxLayout(center)
        v.setContentsMargins(15, 15, 15, 15)
        v.setSpacing(0)

        # история
        self.chat_container = QWidget()
        self.chat_container.setAttribute(Qt.WA_StyledBackground, True)
        self.chat_container.setStyleSheet("background:transparent;")
        self.chat_layout = QVBoxLayout(self.chat_container)
        self.chat_layout.setContentsMargins(15, 15, 15, 10)
        self.chat_layout.setSpacing(10)
        self.chat_layout.addStretch()

        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setWidget(self.chat_container)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.scroll_area.setAttribute(Qt.WA_StyledBackground, True)
        self.scroll_area.setStyleSheet("""
            QScrollArea{
                border:none;
                background:#2E2E2E;
                border-radius:12px;
            }
            """)
        v.addWidget(self.scroll_area, 1)

        h = QHBoxLayout()
        h.setSpacing(15)
        v.addLayout(h)

        self.input_container = QWidget()
        self.input_container.setAttribute(Qt.WA_StyledBackground, True)
        self.input_container.setStyleSheet("""
                border:2px solid #ccc;
                border-radius:14px;
                background:#fff;
            """)
        ic = QHBoxLayout(self.input_container)
        ic.setContentsMargins(5, 5, 5, 5)

        self.input_line = InputTextEdit()
        self.input_line.setFixedHeight(45)
        self.input_line.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
        self.input_line.setPlaceholderText("Введите сообщение…")
        self.input_line.setStyleSheet("border:none;background:transparent;font:15px 'Segoe UI';")
        self.input_line.textChanged.connect(self._adjust_input_height)
        self.input_line.send_signal.connect(self._send_message)
        ic.addWidget(self.input_line)
        h.addWidget(self.input_container, 1)

        def round_button(btn: QPushButton, bg: str) -> QPushButton:
            btn.setFixedSize(45, 45)
            btn.setCursor(Qt.PointingHandCursor)
            btn.setStyleSheet(f"background:{bg};color:#fff;font:20px 'Segoe UI';border:none;border-radius:22px;")
            return btn

        self.send_btn = round_button(QPushButton("➤"), "#10A37F")
        self.send_btn.clicked.connect(self._send_message)
        self.clear_btn = round_button(QPushButton("🗑"), "#4A5568")
        self.clear_btn.clicked.connect(self._clear_chat_ui)
        h.addWidget(self.send_btn)
        h.addWidget(self.clear_btn)

        return center

    # ───────────────── UI helpers ─────────────────
    def _adjust_input_height(self) -> None:
        doc_h = self.input_line.document().size().height()
        new_h = max(45, min(250, int(doc_h) + 10))
        if self.input_line.height() != new_h:
            self.input_line.setFixedHeight(new_h)
            self.input_container.setFixedHeight(new_h + 8)

    def _scroll_bottom(self) -> None:
        bar = self.scroll_area.verticalScrollBar()
        bar.setValue(bar.maximum())

    # ───────────────── dialogs ─────────────────
    def _new_chat(self) -> None:
        self.dialogs.append({"title": f"Чат {len(self.dialogs) + 1}", "messages": []})
        item = QListWidgetItem(self.dialogs[-1]["title"])
        item.setFlags(item.flags() | Qt.ItemIsEditable)
        self.chat_list.addItem(item)
        self.chat_list.setCurrentItem(item)
        self._save_dialogs()

    def _load_chat(self, row: int) -> None:
        if row < 0 or row >= len(self.dialogs):
            return
        self.current_index = row
        for i in reversed(range(self.chat_layout.count() - 1)):
            widget = self.chat_layout.itemAt(i).widget()
            if widget is not None:
                widget.setParent(None)
        for sender, text, user in self.dialogs[row]["messages"]:
            self._create_bubble(sender, text, user)
        QTimer.singleShot(0, self._scroll_bottom)

    def _create_bubble(self, sender: str, text: str, user: bool) -> None:
        bubble = MessageBubble(sender, text, user, max_width=int(self.COLUMN_WIDTH * (0.6 if user else 0.8)))
        self.chat_layout.insertWidget(self.chat_layout.count() - 1, bubble)

    def _append(self, sender: str, text: str, user: bool) -> None:
        self.dialogs[self.current_index]["messages"].append([sender, text, user])
        self._create_bubble(sender, text, user)
        QTimer.singleShot(0, self._scroll_bottom)
        self._save_dialogs()

    # ───────────────── messaging ─────────────────
    def _send_message(self) -> None:
        msg = self.input_line.toPlainText().strip()
        if not msg:
            return
        self._append("Вы", msg, True)
        self.input_line.clear()
        threading.Thread(target=self._ask_openai, args=(msg,), daemon=True).start()

    def _clear_chat_ui(self) -> None:
        if self.current_index < 0:
            return
        self.dialogs[self.current_index].clear()
        self._load_chat(self.current_index)
        self._save_dialogs()

    def _ask_openai(self, prompt: str) -> None:
        try:
            rsp = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=5000,
            )
            answer = rsp.choices[0].message.content.strip()
            self.signals.finished.emit(answer)
        except Exception as e:
            self.signals.error.emit(str(e))

    # ───────────────── signals ─────────────────
    def _connect_signals(self) -> None:
        self.signals = WorkerSignals()
        self.signals.finished.connect(lambda txt: self._append("TLawman", txt, False))
        self.signals.error.connect(lambda err: self._append("Ошибка", err, False))

    def _save_dialogs(self) -> None:
        try:
            with open(SAVE_PATH, "w", encoding="utf-8") as f:
                json.dump(self.dialogs, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"[Ошибка при сохранении]: {e}")

    def _on_title_changed(self, item: QListWidgetItem) -> None:
        row = self.chat_list.row(item)
        if 0 <= row < len(self.dialogs):
            self.dialogs[row]["title"] = item.text()
            self._save_dialogs()

    def _load_dialogs_from_file(self) -> None:
        if not os.path.exists(SAVE_PATH):
            return

        try:
            with open(SAVE_PATH, "r", encoding="utf-8") as f:
                self.dialogs = json.load(f)

            for dialog in self.dialogs:
                item = QListWidgetItem(dialog["title"])
                item.setFlags(item.flags() | Qt.ItemIsEditable)
                self.chat_list.addItem(item)

            if self.dialogs:
                self.chat_list.setCurrentRow(0)

        except Exception as e:
            print(f"[Ошибка при загрузке]: {e}")
            self.dialogs = []


# ────────────────────────────── run ──────────────────────────────
if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyleSheet("""
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
    """)
    win = ChatGPTApp()
    win.show()
    sys.exit(app.exec())
