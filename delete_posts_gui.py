from __future__ import annotations

import argparse
import asyncio
import io
import math
import os
import queue
import re
import sys
import threading
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from telethon import TelegramClient, utils as telethon_utils
from telethon.errors import (
    FloodWaitError,
    PasswordHashInvalidError,
    PhoneCodeExpiredError,
    PhoneCodeInvalidError,
    SessionPasswordNeededError,
)
from telethon.tl.types import Channel

from PySide6.QtCore import QDateTime, QObject, QSettings, QThread, QTimer, Qt, Signal, Slot
from PySide6.QtGui import QBrush, QColor, QFont, QIntValidator, QPalette, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QBoxLayout,
    QCheckBox,
    QComboBox,
    QDateTimeEdit,
    QDialog,
    QFrame,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPlainTextEdit,
    QProgressBar,
    QPushButton,
    QRadioButton,
    QScrollArea,
    QSpinBox,
    QVBoxLayout,
    QWidget,
)

QT6 = True

APP_NAME = "Telegram Cleaner"
ORG_NAME = "MDRK Studio"
APP_DIR_NAME = "MDRKTelegramCleaner"
MAX_DELETE_MESSAGES = 100_000


def is_macos() -> bool:
    return sys.platform == "darwin"


def is_windows() -> bool:
    return sys.platform.startswith("win")


def platform_font_family() -> str:
    if is_macos():
        return ".AppleSystemUIFont"
    if is_windows():
        return "Segoe UI"
    return "Inter"


def platform_mono_font_family() -> str:
    if is_macos():
        return "Menlo"
    if is_windows():
        return "Consolas"
    return "DejaVu Sans Mono"


def data_directory() -> Path:
    if os.environ.get("TELEGRAM_CLEANER_HOME"):
        return Path(os.environ["TELEGRAM_CLEANER_HOME"]).expanduser().resolve()
    if is_macos():
        base_dir = Path.home() / "Library" / "Application Support" / APP_DIR_NAME
    elif is_windows():
        base_dir = Path(os.environ.get("APPDATA", str(Path.home()))) / APP_DIR_NAME
    else:
        base_dir = Path(os.environ.get("XDG_DATA_HOME", str(Path.home() / ".local" / "share"))) / APP_DIR_NAME
    return base_dir


def default_session_path() -> str:
    return str(data_directory() / "telegram_cleaner.session")


def qt_password_mode() -> QLineEdit.EchoMode:
    return QLineEdit.EchoMode.Password if QT6 else QLineEdit.Password


def qt_normal_mode() -> QLineEdit.EchoMode:
    return QLineEdit.EchoMode.Normal if QT6 else QLineEdit.Normal


def qt_align_center() -> Qt.AlignmentFlag | Qt.Alignment:
    return Qt.AlignmentFlag.AlignCenter if QT6 else Qt.AlignCenter


def qt_box_left_to_right() -> QBoxLayout.Direction:
    return QBoxLayout.Direction.LeftToRight if QT6 else QBoxLayout.LeftToRight


def qt_box_top_to_bottom() -> QBoxLayout.Direction:
    return QBoxLayout.Direction.TopToBottom if QT6 else QBoxLayout.TopToBottom


def qt_keep_aspect_ratio() -> Qt.AspectRatioMode | Qt.AspectRatioMode:
    return Qt.AspectRatioMode.KeepAspectRatio if QT6 else Qt.KeepAspectRatio


def qt_smooth_transformation() -> Qt.TransformationMode | Qt.TransformationMode:
    return Qt.TransformationMode.SmoothTransformation if QT6 else Qt.SmoothTransformation


def qt_foreground_role() -> Qt.ItemDataRole | int:
    return Qt.ItemDataRole.ForegroundRole if QT6 else Qt.ForegroundRole


def palette_placeholder_role() -> QPalette.ColorRole:
    return QPalette.ColorRole.PlaceholderText if QT6 else QPalette.PlaceholderText


def local_qdatetime_to_utc(value: QDateTime) -> datetime:
    return datetime.fromtimestamp(value.toSecsSinceEpoch(), tz=timezone.utc)


def mask_phone(value: str) -> str:
    digits = "".join(ch for ch in value if ch.isdigit())
    if len(digits) < 7:
        return "••••"
    return f"+{digits[:1]}•••{digits[-4:]}"


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 8:
        return "••••••"
    return f"{value[:3]}••••{value[-3:]}"


def make_label(text: str, kind: str = "normal") -> QLabel:
    label = QLabel(text)
    if kind == "muted":
        label.setProperty("muted", True)
    if kind == "title":
        label.setProperty("title", True)
    if kind == "status":
        label.setProperty("status", True)
    if kind == "metric":
        label.setProperty("metric", True)
    return label


def tune_placeholder(widget: QLineEdit, color: str = "#5f6368") -> None:
    palette = widget.palette()
    palette.setColor(palette_placeholder_role(), QColor(color))
    widget.setPalette(palette)


def tune_combo(widget: QComboBox) -> None:
    palette = widget.palette()
    for group in (QPalette.ColorGroup.Active, QPalette.ColorGroup.Inactive) if QT6 else (QPalette.Active, QPalette.Inactive):
        palette.setColor(group, QPalette.ColorRole.Text if QT6 else QPalette.Text, QColor("#1d1d1f"))
        palette.setColor(group, QPalette.ColorRole.ButtonText if QT6 else QPalette.ButtonText, QColor("#1d1d1f"))
        palette.setColor(group, QPalette.ColorRole.Base if QT6 else QPalette.Base, QColor("#ffffff"))
    widget.setPalette(palette)
    widget.view().setPalette(palette)


@dataclass(frozen=True)
class CleanerConfig:
    api_id: int
    api_hash: str
    phone: str
    session_path: str
    target: str
    use_qr_login: bool
    start_utc: Optional[datetime]
    end_utc: Optional[datetime]
    batch_size: int
    max_messages: int
    only_outgoing: bool
    dry_run: bool
    max_retries: int
    retry_delay_seconds: int
    batch_cooldown_seconds: int
    request_timeout_seconds: int
    scan_wait_seconds: int


@dataclass(frozen=True)
class MatchResult:
    matched: bool
    skipped_reason: str = ""
    should_stop: bool = False


class TelegramWorker(QObject):
    log = Signal(str)
    status = Signal(str)
    progress = Signal(int, int, int, int)
    finished = Signal(str, int, int, int)
    failed = Signal(str)
    dialogs_loaded = Signal(list)
    code_requested = Signal(str)
    password_requested = Signal()
    qr_requested = Signal(str, str)
    qr_closed = Signal()
    authorized = Signal()
    confirmation_requested = Signal(str, str, int, str)

    def __init__(self, config: CleanerConfig, task: str) -> None:
        super().__init__()
        self.config = config
        if task not in {"scan", "delete", "dialogs"}:
            raise ValueError("Неизвестная операция")
        self.task = task
        self._matched_ids: list[int] = []
        self._approved_ids: frozenset[int] = frozenset()
        self._confirmed_peer_id: Optional[int] = None
        self._confirmation_queue: queue.Queue[bool] = queue.Queue(maxsize=1)
        self._cancelled = threading.Event()
        self._code_queue: queue.Queue[Optional[str]] = queue.Queue(maxsize=1)
        self._password_queue: queue.Queue[Optional[str]] = queue.Queue(maxsize=1)

    @Slot()
    def run(self) -> None:
        try:
            asyncio.run(self._run_async())
        except Exception as exc:
            if self._cancelled.is_set():
                self.finished.emit("Отменено", 0, 0, 0)
                return
            detail = "".join(traceback.format_exception_only(type(exc), exc)).strip()
            self.failed.emit(detail)

    def cancel(self) -> None:
        self._cancelled.set()
        self._offer_value(self._code_queue, None)
        self._offer_value(self._password_queue, None)
        self._offer_value(self._confirmation_queue, False)

    def submit_code(self, code: Optional[str]) -> None:
        self._offer_value(self._code_queue, code)

    def submit_password(self, password: Optional[str]) -> None:
        self._offer_value(self._password_queue, password)

    def submit_confirmation(self, accepted: bool) -> None:
        self._offer_value(self._confirmation_queue, accepted)

    async def _validate_target(self, client: TelegramClient, entity: Any) -> None:
        if not isinstance(entity, Channel) or not (entity.broadcast or entity.megagroup):
            raise ValueError("Выберите канал или супергруппу, которыми вы управляете.")
        permissions = await self._with_retries(
            "получение прав", lambda: client.get_permissions(entity, "me")
        )
        if not permissions.is_admin:
            raise ValueError("Нужны права администратора или владельца канала.")
        if self.task == "delete" and not self.config.dry_run:
            if not (permissions.is_creator or permissions.delete_messages):
                raise ValueError("У аккаунта нет права удалять сообщения в этом канале.")

    async def _run_async(self) -> None:
        config = self.config
        session_path = Path(config.session_path).expanduser()
        if not str(session_path).endswith(".session"):
            session_path = Path(str(session_path) + ".session")
        session_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        session = str(session_path)

        client_kwargs = {
            "request_retries": max(1, config.max_retries),
            "connection_retries": max(1, config.max_retries),
            "retry_delay": max(1, config.retry_delay_seconds),
            "timeout": max(5, config.request_timeout_seconds),
        }

        client = TelegramClient(session, config.api_id, config.api_hash, **client_kwargs)
        if os.name != "nt" and session_path.exists():
            session_path.chmod(0o600)
        self.status.emit("Подключение к Telegram...")
        try:
            await self._with_retries("подключение", client.connect)
            await self._ensure_authorized(client)

            if self.task == "dialogs":
                await self._load_dialogs(client)
                self.finished.emit("Список загружен", 0, 0, 0)
                return

            self.status.emit("Поиск цели...")
            entity = await self._with_retries("поиск цели", lambda: client.get_entity(self._target_value()))
            await self._validate_target(client, entity)
            target_name = self._entity_name(entity)
            self.log.emit(f"Цель: {target_name}")
            self.log.emit(f"Фильтр: {self._scope_text()}")

            scanned, matched, skipped = await self._count_matching_messages(client, entity)
            if self._cancelled.is_set():
                self.finished.emit("Отменено", scanned, matched, 0)
                return

            self.log.emit(f"Подсчёт завершён: найдено к удалению {matched}, пропущено {skipped}.")

            if self.task == "scan" or config.dry_run:
                if config.dry_run and self.task == "delete":
                    self.log.emit("Подсчёт завершён.")
                self.finished.emit("Готово", scanned, matched, 0)
                return

            if matched == 0:
                self.finished.emit("Готово", scanned, matched, 0)
                return

            peer_id = telethon_utils.get_peer_id(entity)
            self.status.emit("Ожидание подтверждения...")
            self.confirmation_requested.emit(target_name, str(peer_id), matched, self._scope_text())
            accepted = False
            while not self._cancelled.is_set():
                try:
                    accepted = self._confirmation_queue.get_nowait()
                    break
                except queue.Empty:
                    await asyncio.sleep(0.1)
            if not accepted or self._cancelled.is_set():
                self.finished.emit("Отменено", scanned, matched, 0)
                return
            await self._validate_target(client, entity)
            self._confirmed_peer_id = peer_id
            self._approved_ids = frozenset(self._matched_ids)
            self.log.emit("Начинаю удаление для всех.")
            deleted, delete_scanned = await self._delete_matching_messages(client, entity, matched)
            final_scanned = max(scanned, delete_scanned)
            status_text = "Отменено" if self._cancelled.is_set() else (
                "Готово" if deleted == matched else "Завершено с пропусками"
            )
            self.finished.emit(status_text, final_scanned, matched, deleted)
        finally:
            if client.is_connected():
                await client.disconnect()
            if os.name != "nt" and session_path.exists():
                session_path.chmod(0o600)

    async def _ensure_authorized(self, client: TelegramClient) -> None:
        if await client.is_user_authorized():
            self.log.emit("Сессия уже авторизована.")
            self.authorized.emit()
            return

        if self.config.use_qr_login:
            await self._sign_in_with_qr(client)
            self.authorized.emit()
            return

        if not self.config.phone:
            raise RuntimeError("Сессия не авторизована. Укажите телефон или выберите вход по QR-коду.")

        self.log.emit("Отправляю код подтверждения...")
        sent_code = await self._with_retries("отправка кода", lambda: client.send_code_request(self.config.phone))

        for attempt in range(1, 4):
            self.code_requested.emit(self.config.phone)
            code = self._code_queue.get()
            if not code:
                raise RuntimeError("Вход отменён: код подтверждения не введён.")

            try:
                await asyncio.wait_for(
                    client.sign_in(
                        phone=self.config.phone,
                        code=code.strip().replace(" ", ""),
                        phone_code_hash=sent_code.phone_code_hash,
                    ),
                    timeout=self.config.request_timeout_seconds,
                )
                self.log.emit("Вход выполнен.")
                self.authorized.emit()
                return
            except PhoneCodeInvalidError:
                self.log.emit(f"Неверный код Telegram, попытка {attempt}/3.")
            except PhoneCodeExpiredError as exc:
                raise RuntimeError("Код Telegram истёк. Нажмите действие ещё раз, чтобы запросить новый код.") from exc
            except SessionPasswordNeededError:
                await self._sign_in_with_password(client)
                self.log.emit("Вход выполнен.")
                self.authorized.emit()
                return

        raise RuntimeError("Вход отменён: Telegram отклонил код 3 раза.")

    async def _sign_in_with_qr(self, client: TelegramClient) -> None:
        self.status.emit("Ожидание QR-входа...")
        self.log.emit("Создаю QR-код для входа.")
        qr_login = await self._with_retries("создание QR-кода", client.qr_login)
        attempts = max(3, self.config.max_retries + 1)

        for attempt in range(1, attempts + 1):
            if self._cancelled.is_set():
                raise RuntimeError("QR-вход отменён пользователем.")

            expires_text = qr_login.expires.astimezone().strftime("%H:%M:%S")
            self.qr_requested.emit(qr_login.url, expires_text)
            self.log.emit(f"QR-код активен до {expires_text}. Отсканируйте его в Telegram.")

            try:
                await self._await_with_cancel(qr_login.wait(timeout=self.config.request_timeout_seconds))
                self.qr_closed.emit()
                self.log.emit("QR-вход выполнен.")
                return
            except asyncio.TimeoutError:
                self.log.emit(f"QR-код истёк или не был подтверждён, попытка {attempt}/{attempts}.")
                if attempt >= attempts:
                    raise RuntimeError("QR-вход не выполнен: время ожидания истекло.")
                await qr_login.recreate()
            except SessionPasswordNeededError:
                self.qr_closed.emit()
                await self._sign_in_with_password(client)
                self.log.emit("QR-вход выполнен.")
                return

    async def _sign_in_with_password(self, client: TelegramClient) -> None:
        self.log.emit("Аккаунт защищён облачным паролем 2FA.")

        for attempt in range(1, 4):
            self.password_requested.emit()
            password = self._password_queue.get()
            if not password:
                raise RuntimeError("Вход отменён: облачный пароль 2FA не введён.")

            try:
                await asyncio.wait_for(
                    client.sign_in(password=password),
                    timeout=self.config.request_timeout_seconds,
                )
                return
            except PasswordHashInvalidError:
                self.log.emit(f"Неверный облачный пароль, попытка {attempt}/3.")

        raise RuntimeError("Вход отменён: Telegram отклонил облачный пароль 3 раза.")

    async def _load_dialogs(self, client: TelegramClient) -> None:
        self.status.emit("Загрузка доступных каналов...")
        self.progress.emit(0, 0, 0, 0)
        items: list[dict[str, str]] = []

        async for dialog in client.iter_dialogs():
            if self._cancelled.is_set():
                break

            entity = dialog.entity
            if not isinstance(entity, Channel):
                continue

            is_broadcast = bool(getattr(entity, "broadcast", False))
            is_megagroup = bool(getattr(entity, "megagroup", False))
            if not is_broadcast and not is_megagroup:
                continue

            role = self._admin_role(entity)
            if not role:
                continue

            title = self._entity_name(entity)
            username = getattr(entity, "username", None)
            peer_id = telethon_utils.get_peer_id(entity)
            target = str(peer_id)
            kind = "канал" if is_broadcast else "группа"
            username_text = f"@{username}" if username else "закрытый"
            display = f"{title} · {role} · {kind} · {username_text}"
            items.append({"display": display, "target": target, "title": title, "role": role})

        self.dialogs_loaded.emit(items)
        self.log.emit(f"Загружено каналов/групп с правами администратора: {len(items)}")

    async def _count_matching_messages(self, client: TelegramClient, entity: Any) -> tuple[int, int, int]:
        self.status.emit("Подсчёт сообщений перед удалением...")
        self.progress.emit(0, 0, 0, 0)

        self._matched_ids.clear()
        scanned = 0
        matched = 0
        skipped = 0
        skipped_logged = 0

        async for message in client.iter_messages(entity, wait_time=self._scan_wait()):
            if self._cancelled.is_set():
                self.log.emit("Операция отменена пользователем.")
                break

            scanned += 1
            result = self._match_message(message)

            if result.should_stop:
                self.log.emit("Достигнута нижняя граница даты, дальнейшее сканирование остановлено.")
                break

            if not result.matched:
                skipped += 1
                if result.skipped_reason and skipped_logged < 10:
                    self.log.emit(f"Пропуск #{message.id}: {result.skipped_reason}")
                    skipped_logged += 1
                self._emit_progress(scanned, matched, 0, 0)
                continue

            matched += 1
            if self.task == "delete" and not self.config.dry_run:
                if matched > MAX_DELETE_MESSAGES:
                    self._matched_ids.clear()
                    raise ValueError("Больше 100 000 сообщений. Уточните период или задайте лимит.")
                self._matched_ids.append(message.id)
            self._emit_progress(scanned, matched, 0, 0)

            if self.config.max_messages and matched >= self.config.max_messages:
                self.log.emit(f"Достигнут лимит сообщений: {self.config.max_messages}.")
                break

        if skipped > skipped_logged:
            self.log.emit(f"Ещё пропусков без подробного вывода: {skipped - skipped_logged}.")

        return scanned, matched, skipped

    async def _delete_matching_messages(
        self,
        client: TelegramClient,
        entity: Any,
        total_to_delete: int,
    ) -> tuple[int, int]:
        self.status.emit("Удаление сообщений...")
        self.progress.emit(0, total_to_delete, 0, total_to_delete)

        deleted = 0
        processed = 0
        ids = tuple(self._matched_ids)
        total_batches = max(1, math.ceil(len(ids) / self.config.batch_size))
        for offset in range(0, len(ids), self.config.batch_size):
            if self._cancelled.is_set():
                break
            batch = list(ids[offset:offset + self.config.batch_size])
            deleted += await self._delete_batch(
                client, entity, batch, offset // self.config.batch_size + 1, total_batches
            )
            processed += len(batch)
            self._emit_progress(processed, total_to_delete, deleted, total_to_delete)
            if processed < len(ids):
                await self._cooldown_after_batch()
        return deleted, processed

    async def _delete_batch(
        self,
        client: TelegramClient,
        entity: Any,
        ids: list[int],
        batch_number: int,
        total_batches: int,
    ) -> int:
        label = f"пачка {batch_number}/{total_batches}, сообщений: {len(ids)}"
        success, error = await self._try_delete_ids(client, entity, ids, label)
        if success:
            return len(ids)

        self.log.emit(f"Ошибка пачки, пробую удалить по одному: {self._short_error(error)}")
        deleted = 0
        for message_id in ids:
            if self._cancelled.is_set():
                break
            success, error = await self._try_delete_ids(client, entity, [message_id], f"сообщение {message_id}")
            if success:
                deleted += 1
            else:
                self.log.emit(f"Пропуск сообщения {message_id}: {self._short_error(error)}")
        return deleted

    async def _try_delete_ids(
        self,
        client: TelegramClient,
        entity: Any,
        ids: list[int],
        label: str,
    ) -> tuple[bool, Optional[BaseException]]:
        if (
            self.task != "delete" or self.config.dry_run
            or not isinstance(entity, Channel)
            or self._confirmed_peer_id != telethon_utils.get_peer_id(entity)
            or not ids or not set(ids).issubset(self._approved_ids)
        ):
            raise RuntimeError("Удаление не подтверждено для выбранного канала и сообщений.")
        last_error: Optional[BaseException] = None
        attempts = self.config.max_retries + 1

        for attempt in range(1, attempts + 1):
            if self._cancelled.is_set():
                return False, RuntimeError("отменено пользователем")

            try:
                await asyncio.wait_for(
                    client.delete_messages(entity, ids, revoke=True),
                    timeout=self.config.request_timeout_seconds,
                )
                self.log.emit(f"Удалено: {label}")
                return True, None
            except FloodWaitError as exc:
                last_error = exc
                wait_seconds = int(getattr(exc, "seconds", 0)) + self.config.retry_delay_seconds
                self.log.emit(f"FloodWait на {label}: пауза {wait_seconds} сек.")
                await self._sleep_with_cancel(wait_seconds)
            except (asyncio.TimeoutError, OSError, RuntimeError) as exc:
                last_error = exc
                if attempt < attempts:
                    self.log.emit(
                        f"Ошибка на {label}, попытка {attempt}/{attempts}: "
                        f"{self._short_error(exc)}. Ретрай через {self.config.retry_delay_seconds} сек."
                    )
                    await self._sleep_with_cancel(self.config.retry_delay_seconds)
            except Exception as exc:
                last_error = exc
                if attempt < attempts:
                    self.log.emit(
                        f"Ошибка Telegram на {label}, попытка {attempt}/{attempts}: "
                        f"{self._short_error(exc)}"
                    )
                    await self._sleep_with_cancel(self.config.retry_delay_seconds)

        return False, last_error

    async def _with_retries(self, operation_name: str, factory: Callable[[], Any]) -> Any:
        last_error: Optional[BaseException] = None
        attempts = self.config.max_retries + 1

        for attempt in range(1, attempts + 1):
            if self._cancelled.is_set():
                raise RuntimeError("операция отменена пользователем")

            try:
                return await self._await_with_cancel(
                    asyncio.wait_for(factory(), timeout=self.config.request_timeout_seconds)
                )
            except FloodWaitError as exc:
                last_error = exc
                wait_seconds = int(getattr(exc, "seconds", 0)) + self.config.retry_delay_seconds
                self.log.emit(f"FloodWait: {operation_name}, пауза {wait_seconds} сек.")
                await self._sleep_with_cancel(wait_seconds)
            except SessionPasswordNeededError:
                raise
            except (asyncio.TimeoutError, OSError, RuntimeError) as exc:
                last_error = exc
                if attempt < attempts:
                    self.log.emit(
                        f"{operation_name}: попытка {attempt}/{attempts} не удалась "
                        f"({self._short_error(exc)}), ретрай через {self.config.retry_delay_seconds} сек."
                    )
                    await self._sleep_with_cancel(self.config.retry_delay_seconds)
            except Exception as exc:
                last_error = exc
                if attempt < attempts:
                    self.log.emit(
                        f"{operation_name}: ошибка {self._short_error(exc)}, "
                        f"ретрай через {self.config.retry_delay_seconds} сек."
                    )
                    await self._sleep_with_cancel(self.config.retry_delay_seconds)

        raise RuntimeError(f"{operation_name}: все попытки исчерпаны: {self._short_error(last_error)}")

    async def _await_with_cancel(self, awaitable: Any) -> Any:
        task = asyncio.ensure_future(awaitable)
        try:
            while not task.done():
                if self._cancelled.is_set():
                    raise RuntimeError("Операция отменена пользователем.")
                await asyncio.wait({task}, timeout=0.1)
            return task.result()
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def _target_value(self) -> str | int:
        target = self.config.target.strip()
        if target.lstrip("-").isdigit():
            return int(target)
        return target

    def _match_message(self, message: Any) -> MatchResult:
        message_date = self._normalize_message_date(message.date)

        if self.config.end_utc and message_date > self.config.end_utc:
            return MatchResult(False, "новее даты 'до'")

        if self.config.start_utc and message_date < self.config.start_utc:
            return MatchResult(False, "старше даты 'после'", True)

        if self.config.only_outgoing and not getattr(message, "out", False):
            return MatchResult(False, "не исходящее сообщение")

        return MatchResult(True)

    def _scope_text(self) -> str:
        parts: list[str] = []
        if self.config.start_utc:
            parts.append("после " + self.config.start_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S"))
        if self.config.end_utc:
            parts.append("до " + self.config.end_utc.astimezone().strftime("%Y-%m-%d %H:%M:%S"))
        if self.config.only_outgoing:
            parts.append("только исходящие")
        if self.config.max_messages:
            parts.append(f"лимит {self.config.max_messages}")
        return ", ".join(parts) if parts else "все сообщения"

    def _scan_wait(self) -> Optional[int]:
        return self.config.scan_wait_seconds or None

    async def _cooldown_after_batch(self) -> None:
        if self.config.batch_cooldown_seconds > 0:
            await self._sleep_with_cancel(self.config.batch_cooldown_seconds)

    async def _sleep_with_cancel(self, seconds: int) -> None:
        remaining = max(0, int(seconds))
        while remaining > 0 and not self._cancelled.is_set():
            chunk = min(1, remaining)
            await asyncio.sleep(chunk)
            remaining -= chunk

    @staticmethod
    def _normalize_message_date(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _entity_name(entity: Any) -> str:
        for attr in ("title", "username", "first_name"):
            value = getattr(entity, attr, None)
            if value:
                return str(value).replace("\n", " ").strip()
        return repr(entity)

    @staticmethod
    def _admin_role(entity: Any) -> str:
        if bool(getattr(entity, "creator", False)):
            return "владелец"
        if getattr(entity, "admin_rights", None) is not None:
            return "администратор"
        return ""

    @staticmethod
    def _short_error(error: Optional[BaseException]) -> str:
        if error is None:
            return "неизвестная ошибка"
        return f"{type(error).__name__}: {error}"

    @staticmethod
    def _offer_value(target_queue: queue.Queue[Any], value: Any) -> None:
        try:
            target_queue.put_nowait(value)
        except queue.Full:
            pass

    def _emit_progress(self, scanned: int, matched: int, deleted: int, total: int) -> None:
        if total > 0:
            self.progress.emit(scanned, matched, deleted, total)
            return

        if scanned <= 5 or 0 < matched <= 5 or scanned % 25 == 0 or (matched > 0 and matched % 25 == 0):
            self.progress.emit(scanned, matched, deleted, total)


class MainWindow(QMainWindow):
    def __init__(self, default_dry_run: bool = False) -> None:
        super().__init__()
        self.default_dry_run = default_dry_run
        self._running = False
        self._close_when_idle = False
        self.confirmation_dialog: Optional[QDialog] = None
        settings_dir = data_directory()
        settings_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.settings = QSettings(str(settings_dir / "settings.ini"), QSettings.Format.IniFormat)
        self.worker: Optional[TelegramWorker] = None
        self.thread: Optional[QThread] = None
        self.qr_dialog: Optional[QDialog] = None
        self.qr_image_label: Optional[QLabel] = None
        self.qr_info_label: Optional[QLabel] = None
        self.sensitive_values: list[str] = []
        self.sensitive_inputs_masked = False
        self.compact_layout = False
        self.content_layout: Optional[QBoxLayout] = None

        self.setWindowTitle("Telegram Cleaner")
        self.resize(1080, 820)
        self.setMinimumSize(900, 660)

        self._build_ui()
        self._apply_styles()
        self._load_settings()
        self._sync_auth_controls()
        self._sync_date_controls()
        self._sync_delete_button()

    def _build_ui(self) -> None:
        page = QWidget()
        page.setObjectName("page")
        outer = QVBoxLayout(page)
        outer.setContentsMargins(30, 24, 30, 24)
        outer.setSpacing(18)

        header = QVBoxLayout()
        title = make_label("Telegram Cleaner", "title")
        subtitle = make_label(
            "Сообщения и публикации ваших каналов",
            "muted",
        )
        header.addWidget(title)
        header.addWidget(subtitle)
        outer.addLayout(header)

        self.content_layout = QBoxLayout(qt_box_left_to_right())
        self.content_layout.setSpacing(16)
        self.content_layout.addWidget(self._build_form_card(), 2)
        self.content_layout.addWidget(self._build_status_card(), 1)
        outer.addLayout(self.content_layout, 1)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setObjectName("mainScroll")
        scroll.setFrameShape(QFrame.Shape.NoFrame if QT6 else QFrame.NoFrame)
        scroll.setWidget(page)
        self.setCentralWidget(scroll)
        self._sync_responsive_layout()

    def _build_form_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("card")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(16)

        self.auth_group = self._build_auth_group()
        self.filter_group = self._build_filter_group()
        layout.addWidget(self.auth_group)
        layout.addWidget(self.filter_group)

        self.advanced_toggle = QPushButton("Расширенные настройки")
        self.advanced_toggle.setObjectName("secondaryButton")
        self.advanced_toggle.setCheckable(True)
        self.advanced_toggle.toggled.connect(self._toggle_advanced)
        layout.addWidget(self.advanced_toggle)

        self.advanced_panel = self._build_advanced_panel()
        self.advanced_panel.setVisible(False)
        layout.addWidget(self.advanced_panel)

        buttons = QHBoxLayout()
        self.scan_button = QPushButton("Подсчитать")
        self.delete_button = QPushButton("Удалить...")
        self.cancel_button = QPushButton("Отмена")
        self.cancel_button.setEnabled(False)
        self.scan_button.setObjectName("secondaryButton")
        self.delete_button.setObjectName("dangerButton")
        self.cancel_button.setObjectName("secondaryButton")
        self.scan_button.clicked.connect(lambda: self._start_task("scan"))
        self.delete_button.clicked.connect(lambda: self._start_task("delete"))
        self.cancel_button.clicked.connect(self._cancel_task)
        buttons.addWidget(self.scan_button)
        buttons.addWidget(self.delete_button)
        buttons.addWidget(self.cancel_button)
        layout.addLayout(buttons)

        return card

    def _build_advanced_panel(self) -> QFrame:
        panel = QFrame()
        panel.setObjectName("advancedPanel")
        layout = QVBoxLayout(panel)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)

        advanced_group = QGroupBox("Для опытных пользователей")
        advanced_layout = QGridLayout(advanced_group)
        advanced_layout.setHorizontalSpacing(12)
        advanced_layout.setVerticalSpacing(10)

        advanced_layout.addWidget(QLabel("Сессия"), 0, 0)
        advanced_layout.addWidget(self.session_input, 0, 1)
        advanced_layout.addWidget(QLabel("Цель вручную"), 1, 0)
        advanced_layout.addWidget(self.target_input, 1, 1)
        advanced_layout.addWidget(QLabel("Лимит"), 2, 0)
        advanced_layout.addWidget(self.max_messages_spin, 2, 1)
        advanced_layout.addWidget(QLabel("Размер пачки"), 3, 0)
        advanced_layout.addWidget(self.batch_size_spin, 3, 1)
        advanced_layout.addWidget(self.only_outgoing_checkbox, 4, 0, 1, 2)
        advanced_layout.addWidget(self.dry_run_checkbox, 5, 0, 1, 2)

        layout.addWidget(advanced_group)
        layout.addWidget(self._build_reliability_group())
        return panel

    def _build_auth_group(self) -> QGroupBox:
        auth_group = QGroupBox("Аккаунт")
        auth_layout = QGridLayout(auth_group)
        auth_layout.setHorizontalSpacing(12)
        auth_layout.setVerticalSpacing(10)

        self.api_id_input = QLineEdit()
        self.api_id_input.setValidator(QIntValidator(1, 2_147_483_647, self))
        self.api_id_input.setPlaceholderText("123456")
        tune_placeholder(self.api_id_input)

        self.api_hash_input = QLineEdit()
        self.api_hash_input.setPlaceholderText("0123456789abcdef...")
        self.api_hash_input.setEchoMode(qt_password_mode())
        tune_placeholder(self.api_hash_input)

        self.phone_input = QLineEdit()
        self.phone_input.setPlaceholderText("+79990000000")
        tune_placeholder(self.phone_input)

        self.session_input = QLineEdit()
        self.session_input.setPlaceholderText(default_session_path())
        tune_placeholder(self.session_input)

        self.auth_qr_radio = QRadioButton("QR-код")
        self.auth_code_radio = QRadioButton("Код Telegram")
        self.auth_qr_radio.setChecked(True)
        self.auth_qr_radio.toggled.connect(self._sync_auth_controls)
        auth_method_layout = QHBoxLayout()
        auth_method_layout.addWidget(self.auth_qr_radio)
        auth_method_layout.addWidget(self.auth_code_radio)
        auth_method_layout.addStretch(1)

        self.channel_combo = QComboBox()
        self.channel_combo.addItem("Нажмите «Обновить», чтобы выбрать канал", None)
        self.channel_combo.setItemData(0, QBrush(QColor("#1d1d1f")), qt_foreground_role())
        tune_combo(self.channel_combo)
        self.channel_combo.currentIndexChanged.connect(self._channel_selected)

        self.load_channels_button = QPushButton("Обновить")
        self.load_channels_button.setObjectName("secondaryButton")
        self.load_channels_button.clicked.connect(lambda: self._start_task("dialogs"))

        self.target_input = QLineEdit()
        self.target_input.setPlaceholderText("@channel или -100...")
        self.target_input.textChanged.connect(self._manual_target_changed)
        tune_placeholder(self.target_input)

        auth_layout.addWidget(QLabel("API ID"), 0, 0)
        auth_layout.addWidget(self.api_id_input, 0, 1, 1, 2)
        auth_layout.addWidget(QLabel("API Hash"), 1, 0)
        auth_layout.addWidget(self.api_hash_input, 1, 1, 1, 2)
        auth_layout.addWidget(QLabel("Вход"), 2, 0)
        auth_layout.addLayout(auth_method_layout, 2, 1, 1, 2)
        auth_layout.addWidget(QLabel("Телефон"), 3, 0)
        auth_layout.addWidget(self.phone_input, 3, 1, 1, 2)
        auth_layout.addWidget(QLabel("Канал"), 4, 0)
        auth_layout.addWidget(self.channel_combo, 4, 1)
        auth_layout.addWidget(self.load_channels_button, 4, 2)

        return auth_group

    def _build_filter_group(self) -> QGroupBox:
        filter_group = QGroupBox("Очистка")
        filter_layout = QGridLayout(filter_group)
        filter_layout.setHorizontalSpacing(12)
        filter_layout.setVerticalSpacing(10)

        now = QDateTime.currentDateTime()
        self.all_messages_radio = QRadioButton("Все сообщения")
        self.date_filter_radio = QRadioButton("По датам")
        self.all_messages_radio.setChecked(True)
        self.all_messages_radio.toggled.connect(self._sync_date_controls)
        self.date_filter_radio.toggled.connect(self._sync_date_controls)

        self.start_enabled_checkbox = QCheckBox("С даты")
        self.start_enabled_checkbox.toggled.connect(self._sync_date_controls)
        self.start_datetime = QDateTimeEdit(now.addDays(-7))
        self.start_datetime.setCalendarPopup(True)
        self.start_datetime.setDisplayFormat("yyyy-MM-dd HH:mm:ss")

        self.end_enabled_checkbox = QCheckBox("До даты")
        self.end_enabled_checkbox.toggled.connect(self._sync_date_controls)
        self.end_datetime = QDateTimeEdit(now)
        self.end_datetime.setCalendarPopup(True)
        self.end_datetime.setDisplayFormat("yyyy-MM-dd HH:mm:ss")

        self.date_filter_panel = QFrame()
        self.date_filter_panel.setObjectName("inlinePanel")
        date_filter_layout = QGridLayout(self.date_filter_panel)
        date_filter_layout.setContentsMargins(12, 10, 12, 10)
        date_filter_layout.setHorizontalSpacing(12)
        date_filter_layout.setVerticalSpacing(10)
        date_filter_layout.addWidget(self.start_enabled_checkbox, 0, 0)
        date_filter_layout.addWidget(self.start_datetime, 0, 1)
        date_filter_layout.addWidget(self.end_enabled_checkbox, 1, 0)
        date_filter_layout.addWidget(self.end_datetime, 1, 1)
        self.date_filter_panel.setVisible(False)

        self.only_outgoing_checkbox = QCheckBox("Только мои сообщения")
        self.dry_run_checkbox = QCheckBox("Только подсчёт")
        self.dry_run_checkbox.toggled.connect(self._sync_delete_button)

        self.max_messages_spin = QSpinBox()
        self.max_messages_spin.setRange(0, 1_000_000)
        self.max_messages_spin.setSpecialValueText("без лимита")

        self.batch_size_spin = QSpinBox()
        self.batch_size_spin.setRange(1, 100)
        self.batch_size_spin.setValue(100)

        filter_layout.addWidget(self.all_messages_radio, 0, 0, 1, 2)
        filter_layout.addWidget(self.date_filter_radio, 1, 0, 1, 2)
        filter_layout.addWidget(self.date_filter_panel, 2, 0, 1, 2)

        return filter_group

    def _build_reliability_group(self) -> QGroupBox:
        reliability_group = QGroupBox("Сеть")
        reliability_layout = QGridLayout(reliability_group)
        reliability_layout.setHorizontalSpacing(12)
        reliability_layout.setVerticalSpacing(10)

        self.max_retries_spin = QSpinBox()
        self.max_retries_spin.setRange(0, 10)
        self.max_retries_spin.setValue(3)

        self.retry_delay_spin = QSpinBox()
        self.retry_delay_spin.setRange(1, 300)
        self.retry_delay_spin.setValue(5)
        self.retry_delay_spin.setSuffix(" сек")

        self.batch_cooldown_spin = QSpinBox()
        self.batch_cooldown_spin.setRange(0, 300)
        self.batch_cooldown_spin.setValue(1)
        self.batch_cooldown_spin.setSuffix(" сек")

        self.request_timeout_spin = QSpinBox()
        self.request_timeout_spin.setRange(5, 600)
        self.request_timeout_spin.setValue(60)
        self.request_timeout_spin.setSuffix(" сек")

        self.scan_wait_spin = QSpinBox()
        self.scan_wait_spin.setRange(0, 60)
        self.scan_wait_spin.setValue(0)
        self.scan_wait_spin.setSuffix(" сек")

        reliability_layout.addWidget(QLabel("Ретраи"), 0, 0)
        reliability_layout.addWidget(self.max_retries_spin, 0, 1)
        reliability_layout.addWidget(QLabel("Пауза ретрая"), 1, 0)
        reliability_layout.addWidget(self.retry_delay_spin, 1, 1)
        reliability_layout.addWidget(QLabel("Кулдаун пачек"), 2, 0)
        reliability_layout.addWidget(self.batch_cooldown_spin, 2, 1)
        reliability_layout.addWidget(QLabel("Таймаут запроса"), 3, 0)
        reliability_layout.addWidget(self.request_timeout_spin, 3, 1)
        reliability_layout.addWidget(QLabel("Пауза сканирования"), 4, 0)
        reliability_layout.addWidget(self.scan_wait_spin, 4, 1)

        return reliability_group

    def _build_status_card(self) -> QFrame:
        card = QFrame()
        card.setObjectName("card")
        layout = QVBoxLayout(card)
        layout.setContentsMargins(22, 22, 22, 22)
        layout.setSpacing(14)

        self.status_label = make_label("Готов к работе")
        self.status_label.setProperty("status", True)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_bar.setTextVisible(True)

        stats = QGridLayout()
        stats.setHorizontalSpacing(12)
        stats.setVerticalSpacing(8)
        self.scanned_value = make_label("0", "metric")
        self.matched_value = make_label("0", "metric")
        self.deleted_value = make_label("0", "metric")
        stats.addWidget(make_label("Просмотрено", "muted"), 0, 0)
        stats.addWidget(self.scanned_value, 0, 1)
        stats.addWidget(make_label("К удалению", "muted"), 1, 0)
        stats.addWidget(self.matched_value, 1, 1)
        stats.addWidget(make_label("Удалено", "muted"), 2, 0)
        stats.addWidget(self.deleted_value, 2, 1)

        self.log_output = QPlainTextEdit()
        self.log_output.setObjectName("logOutput")
        self.log_output.setReadOnly(True)
        self.log_output.setPlaceholderText("Подробности операции появятся здесь.")
        self.log_output.setVisible(False)

        self.details_button = QPushButton("Показать подробности")
        self.details_button.setObjectName("secondaryButton")
        self.details_button.setCheckable(True)
        self.details_button.toggled.connect(self._toggle_details)

        layout.addWidget(self.status_label)
        layout.addWidget(self.progress_bar)
        layout.addLayout(stats)
        layout.addWidget(self.details_button)
        layout.addWidget(self.log_output, 1)

        return card

    def _apply_styles(self) -> None:
        base_font = QFont(platform_font_family())
        base_font.setPointSize(12)
        self.setFont(base_font)
        mono_font = platform_mono_font_family()
        stylesheet = """
            QScrollArea#mainScroll, QWidget#page {
                background: #f5f5f7;
            }
            QWidget {
                color: #1d1d1f;
            }
            QLabel[title="true"] {
                font-size: 28px;
                font-weight: 700;
                color: #1d1d1f;
            }
            QLabel[muted="true"] {
                color: #6e6e73;
            }
            QLabel[status="true"] {
                font-size: 17px;
                font-weight: 600;
                color: #1d1d1f;
            }
            QLabel[metric="true"] {
                font-size: 22px;
                font-weight: 650;
                color: #1d1d1f;
            }
            QFrame#card, QGroupBox {
                background: #ffffff;
                border: 1px solid #d2d2d7;
                border-radius: 12px;
            }
            QFrame#card {
                min-width: 360px;
            }
            QFrame#advancedPanel {
                background: transparent;
                border: 0;
            }
            QFrame#inlinePanel {
                background: #f5f5f7;
                border: 1px solid #e1e1e6;
                border-radius: 10px;
            }
            QGroupBox {
                margin-top: 14px;
                padding: 18px 14px 14px 14px;
                font-size: 13px;
                font-weight: 650;
                background: #ffffff;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 6px;
                color: #6e6e73;
            }
            QLineEdit, QComboBox, QDateTimeEdit, QSpinBox, QPlainTextEdit {
                background: #ffffff;
                border: 1px solid #d2d2d7;
                border-radius: 8px;
                padding: 7px 10px;
                selection-background-color: #007aff;
            }
            QLineEdit, QComboBox, QDateTimeEdit, QSpinBox {
                min-height: 26px;
            }
            QLineEdit:disabled, QComboBox:disabled, QDateTimeEdit:disabled, QSpinBox:disabled {
                background: #f7f7fa;
                color: #6e6e73;
                border-color: #e1e1e6;
            }
            QLineEdit:focus, QComboBox:focus, QDateTimeEdit:focus, QSpinBox:focus, QPlainTextEdit:focus {
                border-color: #007aff;
                background: #ffffff;
            }
            QPlainTextEdit {
                font-family: "{mono_font}", Menlo, Consolas, monospace;
                font-size: 12px;
                background: #fbfbfd;
                color: #1d1d1f;
                border-color: #d2d2d7;
            }
            QPushButton {
                background: #007aff;
                color: #ffffff;
                border: 0;
                border-radius: 8px;
                padding: 9px 14px;
                font-weight: 600;
                min-height: 28px;
            }
            QPushButton:hover {
                background: #0a84ff;
            }
            QPushButton:disabled {
                background: #ededf1;
                color: #6e6e73;
            }
            QPushButton#secondaryButton {
                background: #e9e9ed;
                color: #1d1d1f;
            }
            QPushButton#secondaryButton:hover {
                background: #dedee3;
            }
            QPushButton#dangerButton {
                background: #ff3b30;
            }
            QPushButton#dangerButton:hover {
                background: #e0352b;
            }
            QProgressBar {
                background: #e9e9ed;
                border: 0;
                border-radius: 6px;
                height: 22px;
                text-align: center;
            }
            QProgressBar::chunk {
                background: #34c759;
                border-radius: 6px;
            }
            QRadioButton, QCheckBox {
                spacing: 8px;
            }
            QRadioButton:disabled, QCheckBox:disabled {
                color: #6e6e73;
            }
            QComboBox {
                color: #1d1d1f;
            }
            QComboBox::drop-down {
                border: 0;
                width: 28px;
            }
            QComboBox QAbstractItemView {
                background: #ffffff;
                color: #1d1d1f;
                selection-background-color: #007aff;
                selection-color: #ffffff;
                border: 1px solid #d2d2d7;
                outline: 0;
                padding: 4px;
            }
            QComboBox QAbstractItemView::item {
                min-height: 28px;
                padding: 6px 8px;
                color: #1d1d1f;
                background: #ffffff;
            }
            QComboBox QAbstractItemView::item:selected {
                color: #ffffff;
                background: #007aff;
            }
            QScrollBar:vertical {
                background: transparent;
                width: 12px;
                margin: 2px;
            }
            QScrollBar::handle:vertical {
                background: #c7c7cc;
                border-radius: 6px;
                min-height: 36px;
            }
            QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical {
                height: 0;
            }
            """
        self.setStyleSheet(stylesheet.replace("{mono_font}", mono_font))

    def _load_settings(self) -> None:
        self.api_id_input.setText(os.environ.get("TELEGRAM_API_ID", self.settings.value("api_id", "")))
        self.api_hash_input.setText(os.environ.get("TELEGRAM_API_HASH", ""))
        self.phone_input.setText(self.settings.value("phone", ""))
        self.session_input.setText(self.settings.value("session", default_session_path()))
        self.target_input.setText(self.settings.value("target", ""))
        auth_method = self.settings.value("auth_method", "qr")
        self.auth_code_radio.setChecked(auth_method == "code")
        self.auth_qr_radio.setChecked(auth_method != "code")
        delete_mode = self.settings.value("delete_mode", "dates")
        self.date_filter_radio.setChecked(delete_mode == "dates")
        self.all_messages_radio.setChecked(delete_mode != "dates")
        self.start_enabled_checkbox.setChecked(self.settings.value("start_enabled", True, type=bool))
        self.end_enabled_checkbox.setChecked(self.settings.value("end_enabled", False, type=bool))
        self.only_outgoing_checkbox.setChecked(self.settings.value("only_outgoing", False, type=bool))
        self.dry_run_checkbox.setChecked(self.default_dry_run or self.settings.value("dry_run", False, type=bool))
        self.max_messages_spin.setValue(self.settings.value("max_messages", 0, type=int))
        self.batch_size_spin.setValue(self.settings.value("batch_size", 100, type=int))
        self.max_retries_spin.setValue(self.settings.value("max_retries", 3, type=int))
        self.retry_delay_spin.setValue(self.settings.value("retry_delay", 5, type=int))
        self.batch_cooldown_spin.setValue(self.settings.value("batch_cooldown", 1, type=int))
        self.request_timeout_spin.setValue(self.settings.value("request_timeout", 60, type=int))
        self.scan_wait_spin.setValue(self.settings.value("scan_wait", 0, type=int))
        self.dry_run_checkbox.setEnabled(not self.default_dry_run)
        for name in ("start", "end"):
            saved = self.settings.value(name + "_datetime", "")
            value = QDateTime.fromString(saved, Qt.DateFormat.ISODate)
            if value.isValid():
                getattr(self, name + "_datetime").setDateTime(value)

    def _save_settings(self) -> None:
        self.settings.setValue("api_id", self.api_id_input.text().strip())
        self.settings.remove("api_hash")
        self.settings.setValue("phone", self.phone_input.text().strip())
        self.settings.setValue("session", self.session_input.text().strip())
        self.settings.setValue("target", self.target_input.text().strip())
        self.settings.setValue("auth_method", "code" if self.auth_code_radio.isChecked() else "qr")
        self.settings.setValue("delete_mode", "dates" if self.date_filter_radio.isChecked() else "all")
        self.settings.setValue("start_enabled", self.start_enabled_checkbox.isChecked())
        self.settings.setValue("end_enabled", self.end_enabled_checkbox.isChecked())
        self.settings.setValue("only_outgoing", self.only_outgoing_checkbox.isChecked())
        self.settings.setValue("dry_run", self.dry_run_checkbox.isChecked())
        self.settings.setValue("max_messages", self.max_messages_spin.value())
        self.settings.setValue("batch_size", self.batch_size_spin.value())
        self.settings.setValue("max_retries", self.max_retries_spin.value())
        self.settings.setValue("retry_delay", self.retry_delay_spin.value())
        self.settings.setValue("batch_cooldown", self.batch_cooldown_spin.value())
        self.settings.setValue("request_timeout", self.request_timeout_spin.value())
        self.settings.setValue("scan_wait", self.scan_wait_spin.value())
        for name in ("start", "end"):
            editor = getattr(self, name + "_datetime")
            self.settings.setValue(name + "_datetime", editor.dateTime().toString(Qt.DateFormat.ISODate))
        self.settings.sync()
        if os.name != "nt" and Path(self.settings.fileName()).exists():
            Path(self.settings.fileName()).chmod(0o600)

    def _sync_date_controls(self) -> None:
        date_mode = self.date_filter_radio.isChecked()
        self.date_filter_panel.setVisible(date_mode)
        self.start_enabled_checkbox.setEnabled(date_mode)
        self.end_enabled_checkbox.setEnabled(date_mode)
        self.start_datetime.setEnabled(date_mode and self.start_enabled_checkbox.isChecked())
        self.end_datetime.setEnabled(date_mode and self.end_enabled_checkbox.isChecked())

    def _sync_auth_controls(self) -> None:
        self.phone_input.setEnabled(self.auth_code_radio.isChecked())
        if not self.sensitive_inputs_masked:
            self.phone_input.setEchoMode(qt_normal_mode() if self.auth_code_radio.isChecked() else qt_password_mode())
            self.session_input.setEchoMode(qt_normal_mode())

    def _sync_delete_button(self) -> None:
        if hasattr(self, "delete_button"):
            self.delete_button.setEnabled(not self._running and not self.dry_run_checkbox.isChecked())

    def _toggle_advanced(self, visible: bool) -> None:
        self.advanced_panel.setVisible(visible)
        self.advanced_toggle.setText("Скрыть расширенные настройки" if visible else "Расширенные настройки")

    def _toggle_details(self, visible: bool) -> None:
        self.log_output.setVisible(visible)
        self.details_button.setText("Скрыть подробности" if visible else "Показать подробности")

    def _mask_sensitive_inputs(self) -> None:
        self.sensitive_inputs_masked = True
        self.api_hash_input.setEchoMode(qt_password_mode())
        self.phone_input.setEchoMode(qt_password_mode())
        self.session_input.setEchoMode(qt_password_mode())

    def _sanitize_text(self, text: str) -> str:
        result = text
        for value in self.sensitive_values:
            if value and len(value) >= 4:
                result = result.replace(value, mask_secret(value))
        phone = self.phone_input.text().strip()
        if phone:
            result = result.replace(phone, mask_phone(phone))
        return result

    def _collect_sensitive_values(self, config: CleanerConfig) -> None:
        values = [
            config.api_hash,
            config.phone,
            config.session_path,
            config.target,
        ]
        data = self.channel_combo.itemData(self.channel_combo.currentIndex())
        if isinstance(data, dict):
            values.extend(str(data.get(key, "")) for key in ("title", "target", "display"))
        self.sensitive_values = [value for value in values if value and len(value) >= 4]

    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._sync_responsive_layout()

    def _sync_responsive_layout(self) -> None:
        if self.content_layout is None:
            return
        compact = self.width() < 980
        if compact == self.compact_layout:
            return
        self.compact_layout = compact
        self.content_layout.setDirection(qt_box_top_to_bottom() if compact else qt_box_left_to_right())

    def _channel_selected(self, index: int) -> None:
        data = self.channel_combo.itemData(index)
        if isinstance(data, dict) and data.get("target"):
            self.target_input.setText(str(data["target"]))
        else:
            self.target_input.clear()

    def _manual_target_changed(self, text: str) -> None:
        selected = self.channel_combo.currentData()
        if isinstance(selected, dict) and selected.get("target") == text.strip():
            return
        self.channel_combo.blockSignals(True)
        self.channel_combo.setCurrentIndex(0)
        self.channel_combo.setItemText(0, "Цель указана вручную" if text.strip() else "Выберите канал")
        self.channel_combo.blockSignals(False)

    def _collect_config(self, require_target: bool) -> Optional[CleanerConfig]:
        api_id_text = self.api_id_input.text().strip()
        api_hash = self.api_hash_input.text().strip()
        phone = self.phone_input.text().strip()
        session_path = self.session_input.text().strip()
        target = self.target_input.text().strip()

        if not api_id_text or not api_hash or not session_path:
            self._show_error("Заполните API ID, API Hash и файл сессии.")
            return None

        if not re.fullmatch(r"[0-9]+", api_id_text) or not 1 <= int(api_id_text) <= 2_147_483_647:
            self._show_error("API ID должен быть положительным числом.")
            return None
        if not re.fullmatch(r"[a-fA-F0-9]{32}", api_hash):
            self._show_error("API Hash должен содержать 32 шестнадцатеричных символа.")
            return None

        if require_target and not target:
            self._show_error("Выберите канал из списка или укажите цель вручную.")
            return None

        start_utc = None
        end_utc = None
        date_mode = self.date_filter_radio.isChecked()
        if date_mode and require_target and not (self.start_enabled_checkbox.isChecked() or self.end_enabled_checkbox.isChecked()):
            self._show_error("Укажите хотя бы одну границу периода.")
            return None
        if date_mode and self.start_enabled_checkbox.isChecked():
            start_utc = local_qdatetime_to_utc(self.start_datetime.dateTime())
        if date_mode and self.end_enabled_checkbox.isChecked():
            end_utc = local_qdatetime_to_utc(self.end_datetime.dateTime())
        if start_utc and end_utc and start_utc > end_utc:
            self._show_error("Дата 'после' не может быть позже даты 'до'.")
            return None

        return CleanerConfig(
            api_id=int(api_id_text),
            api_hash=api_hash,
            phone=phone,
            session_path=session_path,
            target=target,
            use_qr_login=self.auth_qr_radio.isChecked(),
            start_utc=start_utc,
            end_utc=end_utc,
            batch_size=self.batch_size_spin.value(),
            max_messages=self.max_messages_spin.value(),
            only_outgoing=self.only_outgoing_checkbox.isChecked(),
            dry_run=self.default_dry_run or self.dry_run_checkbox.isChecked(),
            max_retries=self.max_retries_spin.value(),
            retry_delay_seconds=self.retry_delay_spin.value(),
            batch_cooldown_seconds=self.batch_cooldown_spin.value(),
            request_timeout_seconds=self.request_timeout_spin.value(),
            scan_wait_seconds=self.scan_wait_spin.value(),
        )

    def _start_task(self, task: str) -> None:
        if self.worker is not None:
            return

        config = self._collect_config(require_target=task != "dialogs")
        if config is None:
            return

        self._collect_sensitive_values(config)
        self._save_settings()
        self._reset_run_state()
        self._set_running(True)

        self.thread = QThread(self)
        self.worker = TelegramWorker(config, task)
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.log.connect(self._append_log)
        self.worker.status.connect(self._set_status)
        self.worker.progress.connect(self._update_progress)
        self.worker.finished.connect(self._task_finished)
        self.worker.failed.connect(self._task_failed)
        self.worker.dialogs_loaded.connect(self._dialogs_loaded)
        self.worker.confirmation_requested.connect(self._confirm_delete)
        self.worker.code_requested.connect(self._ask_code)
        self.worker.password_requested.connect(self._ask_password)
        self.worker.qr_requested.connect(self._show_qr_login)
        self.worker.qr_closed.connect(self._close_qr_dialog)
        self.worker.authorized.connect(self._mask_sensitive_inputs)
        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.finished.connect(self.worker.deleteLater)
        self.thread.finished.connect(self.thread.deleteLater)
        self.thread.finished.connect(self._clear_thread_refs)

        task_name = {"dialogs": "загрузка каналов", "scan": "подсчёт", "delete": "подготовка удаления"}[task]
        self._append_log("Старт: " + task_name)
        self.thread.start()

    def _reset_run_state(self) -> None:
        self.scanned_value.setText("0")
        self.matched_value.setText("0")
        self.deleted_value.setText("0")
        self.log_output.clear()
        self.progress_bar.setRange(0, 0)
        self.progress_bar.setValue(0)
        if hasattr(self, "details_button"):
            self.details_button.setChecked(False)

    def _set_running(self, running: bool) -> None:
        self._running = running
        self.auth_group.setEnabled(not running)
        self.filter_group.setEnabled(not running)
        self.advanced_panel.setEnabled(not running)
        self.scan_button.setEnabled(not running)
        self._sync_delete_button()
        self.load_channels_button.setEnabled(not running)
        self.cancel_button.setEnabled(running)

    def _cancel_task(self) -> None:
        if self.worker:
            self._append_log("Запрошена отмена...")
            self.worker.cancel()
            self.cancel_button.setEnabled(False)
            if self.confirmation_dialog:
                self.confirmation_dialog.reject()
            self._close_qr_dialog()

    @Slot(str)
    def _set_status(self, text: str) -> None:
        self.status_label.setText(text)

    @Slot(str)
    def _append_log(self, text: str) -> None:
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.appendPlainText(f"[{stamp}] {self._sanitize_text(text)}")

    @Slot(int, int, int, int)
    def _update_progress(self, scanned: int, matched: int, deleted: int, total: int) -> None:
        self.scanned_value.setText(str(scanned))
        self.matched_value.setText(str(matched))
        self.deleted_value.setText(str(deleted))
        if total > 0:
            self.progress_bar.setRange(0, total)
            self.progress_bar.setValue(min(deleted, total))
            self.progress_bar.setFormat(f"{deleted}/{total}")
        else:
            self.progress_bar.setRange(0, 0)
            self.progress_bar.setFormat("Подсчёт...")

    @Slot(str, int, int, int)
    def _task_finished(self, status: str, scanned: int, matched: int, deleted: int) -> None:
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(1)
        self.progress_bar.setFormat(status)
        self.scanned_value.setText(str(scanned))
        self.matched_value.setText(str(matched))
        self.deleted_value.setText(str(deleted))
        self._set_status(status)
        self._append_log(f"{status}. Просмотрено: {scanned}, к удалению: {matched}, удалено: {deleted}.")
        self._close_qr_dialog()

    @Slot(str)
    def _task_failed(self, message: str) -> None:
        self.progress_bar.setRange(0, 1)
        self.progress_bar.setValue(0)
        self.progress_bar.setFormat("Ошибка")
        self._set_status("Ошибка")
        safe_message = self._sanitize_text(message)
        self._append_log("Ошибка: " + safe_message)
        self._close_qr_dialog()
        if not self._close_when_idle:
            self._show_error(safe_message)

    @Slot(list)
    def _dialogs_loaded(self, items: list) -> None:
        current_target = self.target_input.text().strip()
        self.channel_combo.blockSignals(True)
        self.channel_combo.clear()

        if not items:
            self.channel_combo.addItem("Администрируемые каналы не найдены", None)
            self.channel_combo.setItemData(0, QBrush(QColor("#1d1d1f")), qt_foreground_role())
        else:
            self.channel_combo.addItem("Выберите канал", None)
            self.channel_combo.setItemData(0, QBrush(QColor("#1d1d1f")), qt_foreground_role())
            for item in items:
                self.channel_combo.addItem(item["display"], item)
        tune_combo(self.channel_combo)

        selected_index = -1
        if current_target:
            for index in range(self.channel_combo.count()):
                data = self.channel_combo.itemData(index)
                if isinstance(data, dict) and data.get("target") == current_target:
                    selected_index = index
                    break
        if selected_index >= 0:
            self.channel_combo.setCurrentIndex(selected_index)

        self.channel_combo.blockSignals(False)
        if current_target:
            self.target_input.setText(current_target)

    @Slot(str)
    def _ask_code(self, phone: str) -> None:
        if not self.worker:
            return
        prompt = (
            f"Введите одноразовый код Telegram для {mask_phone(phone)}.\n\n"
            "Это не облачный пароль. Если включена 2FA, пароль будет запрошен следующим окном."
        )
        code, ok = QInputDialog.getText(self, "Код из Telegram", prompt)
        self.worker.submit_code(code if ok else None)

    @Slot()
    def _ask_password(self) -> None:
        if not self.worker:
            return
        password, ok = QInputDialog.getText(
            self,
            "Облачный пароль 2FA",
            "Введите облачный пароль Telegram:",
            qt_password_mode(),
        )
        self.worker.submit_password(password if ok else None)

    @Slot(str, str)
    def _show_qr_login(self, url: str, expires_text: str) -> None:
        if self.qr_dialog is None:
            self.qr_dialog = QDialog(self)
            self.qr_dialog.setWindowTitle("Вход по QR-коду")
            self.qr_dialog.setModal(False)
            self.qr_dialog.resize(420, 560)

            layout = QVBoxLayout(self.qr_dialog)
            layout.setContentsMargins(22, 22, 22, 22)
            layout.setSpacing(12)

            title = make_label("Сканируйте QR в Telegram", "status")
            title.setAlignment(qt_align_center())
            instruction = make_label(
                "Telegram на телефоне: Настройки -> Устройства -> Подключить устройство.",
                "muted",
            )
            instruction.setWordWrap(True)
            instruction.setAlignment(qt_align_center())

            self.qr_image_label = QLabel()
            self.qr_image_label.setAlignment(qt_align_center())
            self.qr_image_label.setMinimumSize(300, 300)

            self.qr_info_label = make_label("", "muted")
            self.qr_info_label.setAlignment(qt_align_center())

            cancel_button = QPushButton("Отменить вход")
            cancel_button.clicked.connect(self._cancel_task)
            cancel_button.clicked.connect(self._close_qr_dialog)

            layout.addWidget(title)
            layout.addWidget(instruction)
            layout.addWidget(self.qr_image_label)
            layout.addWidget(self.qr_info_label)
            layout.addWidget(cancel_button)

        pixmap = self._make_qr_pixmap(url)
        if self.qr_image_label is not None:
            if pixmap is not None:
                self.qr_image_label.setPixmap(pixmap)
                self.qr_image_label.setText("")
            else:
                self.qr_image_label.setText("Не удалось создать QR-картинку. Используйте ссылку ниже.")

        if self.qr_info_label is not None:
            self.qr_info_label.setText(f"QR действителен до {expires_text}")

        self.qr_dialog.show()
        self.qr_dialog.raise_()
        self.qr_dialog.activateWindow()

    def _make_qr_pixmap(self, url: str) -> Optional[QPixmap]:
        try:
            import qrcode
        except ImportError:
            return None

        image = qrcode.make(url)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG")
        pixmap = QPixmap()
        pixmap.loadFromData(buffer.getvalue(), "PNG")
        return pixmap.scaled(300, 300, qt_keep_aspect_ratio(), qt_smooth_transformation())

    @Slot()
    def _close_qr_dialog(self) -> None:
        if self.qr_dialog is not None:
            self.qr_dialog.close()
            self.qr_dialog.deleteLater()
            self.qr_dialog = None
            self.qr_image_label = None
            self.qr_info_label = None

    def _clear_thread_refs(self) -> None:
        self.worker = None
        self.thread = None
        self._set_running(False)
        if self._close_when_idle:
            QTimer.singleShot(0, self.close)

    def _show_error(self, text: str) -> None:
        QMessageBox.critical(self, "Ошибка", text)

    @Slot(str, str, int, str)
    def _confirm_delete(self, title: str, peer_id: str, count: int, scope: str) -> None:
        if not self.worker or self.worker._cancelled.is_set() or self._close_when_idle:
            return
        dialog = QDialog(self)
        self.confirmation_dialog = dialog
        dialog.setWindowTitle(f"Удалить {count} сообщений?")
        dialog.setMinimumWidth(460)
        layout = QVBoxLayout(dialog)
        for text in (title, f"ID: {peer_id}", scope, f"К удалению: {count}"):
            label = QLabel(text)
            label.setTextFormat(Qt.TextFormat.PlainText)
            label.setWordWrap(True)
            layout.addWidget(label)
        layout.addWidget(QLabel("Введите ID канала для подтверждения:"))
        entry = QLineEdit()
        entry.setObjectName("confirmationId")
        entry.setPlaceholderText(peer_id)
        layout.addWidget(entry)
        consent = QCheckBox("Понимаю, что сообщения будут удалены для всех")
        layout.addWidget(consent)
        buttons = QHBoxLayout()
        cancel = QPushButton("Отмена")
        confirm = QPushButton("Удалить")
        confirm.setObjectName("dangerButton")
        confirm.setEnabled(False)
        confirm.setAutoDefault(False)
        cancel.setAutoDefault(False)
        def sync():
            confirm.setEnabled(entry.text().strip() == peer_id and consent.isChecked())
        entry.textChanged.connect(sync)
        consent.toggled.connect(sync)
        cancel.clicked.connect(dialog.reject)
        confirm.clicked.connect(dialog.accept)
        buttons.addWidget(cancel)
        buttons.addWidget(confirm)
        layout.addLayout(buttons)
        accepted = dialog.exec() == QDialog.DialogCode.Accepted
        self.confirmation_dialog = None
        dialog.deleteLater()
        if self.worker:
            self.worker.submit_confirmation(accepted)

    def closeEvent(self, event) -> None:
        if self.worker is not None:
            if not self._close_when_idle:
                answer = QMessageBox.question(
                    self, "Операция выполняется",
                    "Отменить операцию и закрыть приложение?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                if answer != QMessageBox.StandardButton.Yes:
                    event.ignore()
                    return
                self._close_when_idle = True
                self._cancel_task()
                self._set_status("Завершение операции...")
            event.ignore()
            return
        self._save_settings()
        event.accept()


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="разрешить только подсчёт сообщений",
    )
    parser.add_argument("--data-dir", type=Path, help="папка сессии и настроек")
    return parser.parse_known_args(argv)


def configure_high_dpi() -> None:
    if QT6:
        return
    for attribute_name in ("AA_EnableHighDpiScaling", "AA_UseHighDpiPixmaps"):
        attribute = getattr(Qt, attribute_name, None)
        if attribute is not None:
            QApplication.setAttribute(attribute, True)


def main() -> int:
    args, qt_args = parse_args(sys.argv[1:])
    if args.data_dir:
        os.environ["TELEGRAM_CLEANER_HOME"] = str(args.data_dir.expanduser().resolve())
    configure_high_dpi()
    app = QApplication([sys.argv[0], *qt_args])
    app.setApplicationName(APP_NAME)
    app.setOrganizationName(ORG_NAME)
    app.setFont(QFont(platform_font_family(), 12))
    window = MainWindow(default_dry_run=args.dry_run)
    window.show()
    return app.exec() if hasattr(app, "exec") else app.exec_()


if __name__ == "__main__":
    raise SystemExit(main())
