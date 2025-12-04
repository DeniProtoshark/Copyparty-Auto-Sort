#!/usr/bin/env python3
# copyparty_sorter.py
# Combined: styled logging + robust file handling for large videos
# v1.5.0

import os
import time
import shutil
import signal
import hashlib
import errno
import random
import argparse
from datetime import datetime
from typing import Optional, Set, Dict
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# ------------------ Defaults / Config ------------------
DEFAULT_WATCH_DIR = Path(r"C:\uploads")
DEFAULT_PHOTOS_ROOT = Path(r"J:\Photos")
DEFAULT_LOGFILE = Path(r"J:\test.log")
WAIT_SEC = 5.0
MAX_TRIES = 10
MAX_WORKERS = 4
MAX_PROCESSING_HISTORY = 1000
COPY_BUFFER_SIZE = 8 * 1024 * 1024  # 8 MB buffer by default for big files
RETRY_ATTEMPTS = 8

ALLOWED_EXT = {
    '.jpg', '.jpeg', '.png', '.cr2', '.cr3', '.nef', '.arw',
    '.mp4', '.mov', '.avi', '.mkv', '.mts', '.m2ts',
    '.webp', '.heic', '.heif',
    '.raf', '.orf', '.rw2', '.dng', '.sr2', '.gif', '.bmp', '.tiff'
}

IGNORE_DIRS = {'.hist', '.tmp', 'temp', 'tmp', 'cache', 'thumbnail', 'thumb'}
IGNORE_PREFIXES = ('.', '~', 'Thumbs.db')
IGNORE_EXT = {'.tmp', '.temp', '.crdownload', '.part', '.lnk'}

# ------------------ Globals (initialized in main) ------------------
WATCH_DIR: Path = DEFAULT_WATCH_DIR
PHOTOS_ROOT: Path = DEFAULT_PHOTOS_ROOT
LOGFILE: Path = DEFAULT_LOGFILE
DRY_RUN = False
CHECKSUM_ON_DUP = True

PROCESSING_FILES: Set[str] = set()
FILE_HISTORY: Dict[str, float] = {}
LOCK = threading.Lock()
STOP_EVENT = threading.Event()

# ------------------ Styled logging helpers ------------------

class Color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
    GRAY = '\033[90m'
    ORANGE = '\033[33m'

class LogArt:
    @staticmethod
    def get_random_emoji() -> str:
        emojis = ["📸", "🎥", "🖼️", "📁", "📂", "✨", "🚀", "⚡", "🔍", "📊",
                  "💾", "💿", "📀", "🗂️", "🗃️", "📅", "📆", "🗓️", "⏱️", "🎯"]
        return random.choice(emojis)

    @staticmethod
    def get_file_emoji(extension: str) -> str:
        emoji_map = {
            '.jpg': '🖼️', '.jpeg': '🖼️', '.png': '🖼️',
            '.cr2': '🎞️', '.cr3': '🎞️', '.nef': '🎞️',
            '.mp4': '🎥', '.mov': '🎥', '.avi': '🎥',
            '.heic': '📱', '.heif': '📱',
            '.webp': '🌐', '.gif': '🌀'
        }
        return emoji_map.get(extension.lower(), '📄')

class LogStyle:
    @staticmethod
    def success(msg: str) -> str:
        styles = [
            f"{Color.GREEN}✓ {msg}{Color.END}",
            f"{Color.GREEN}✅ {msg}{Color.END}",
            f"{Color.GREEN}✨ {msg}{Color.END}",
            f"{Color.GREEN}🌟 {msg}{Color.END}",
            f"{Color.BOLD}{Color.GREEN}➤ {msg}{Color.END}"
        ]
        return random.choice(styles)

    @staticmethod
    def info(msg: str) -> str:
        styles = [
            f"{Color.CYAN}ℹ {msg}{Color.END}",
            f"{Color.BLUE}📘 {msg}{Color.END}",
            f"{Color.DARKCYAN}▪ {msg}{Color.END}",
            f"{Color.CYAN}➤ {msg}{Color.END}"
        ]
        return random.choice(styles)

    @staticmethod
    def warning(msg: str) -> str:
        styles = [
            f"{Color.YELLOW}⚠ {msg}{Color.END}",
            f"{Color.ORANGE}⚠️ {msg}{Color.END}",
            f"{Color.YELLOW}⚠️  {msg}{Color.END}",
        ]
        return random.choice(styles)

    @staticmethod
    def error(msg: str) -> str:
        styles = [
            f"{Color.RED}✗ {msg}{Color.END}",
            f"{Color.RED}❌ {msg}{Color.END}",
            f"{Color.RED}💥 {msg}{Color.END}",
            f"{Color.BOLD}{Color.RED}⚠ {msg}{Color.END}"
        ]
        return random.choice(styles)

    @staticmethod
    def debug(msg: str) -> str:
        styles = [
            f"{Color.GRAY}⚙ {msg}{Color.END}",
            f"{Color.GRAY}🔧 {msg}{Color.END}",
            f"{Color.GRAY}🌀 {msg}{Color.END}"
        ]
        return random.choice(styles)

    @staticmethod
    def processing(msg: str) -> str:
        animations = ['⣾', '⣽', '⣻', '⢿', '⡿', '⣟', '⣯', '⣷']
        anim = random.choice(animations)
        return f"{Color.PURPLE}{anim} {msg}{Color.END}"

    @staticmethod
    def header(msg: str) -> str:
        border = '═' * 3
        return f"{Color.BOLD}{Color.CYAN}{border} {msg} {border}{Color.END}"

class ProgressBar:
    @staticmethod
    def create(progress: float, width: int = 20) -> str:
        filled = int(width * progress)
        empty = width - filled
        bars = ['█', '▓', '▒', '░', '▉', '▊', '▋', '▌', '▍']
        bar_char = random.choice(bars)
        return f"{Color.GREEN}{bar_char * filled}{Color.GRAY}{'░' * empty}{Color.END} {progress:.1%}"

class Statistics:
    def __init__(self):
        self.start_time = time.time()
        self.files_processed = 0
        self.files_moved = 0
        self.files_skipped = 0
        self.errors = 0
        self.by_extension: Dict[str, int] = {}
        self._lock = threading.Lock()

    def add_processed(self, ext: str):
        with self._lock:
            self.files_processed += 1
            self.by_extension[ext] = self.by_extension.get(ext, 0) + 1

    def add_moved(self):
        with self._lock:
            self.files_moved += 1

    def add_skipped(self):
        with self._lock:
            self.files_skipped += 1

    def add_error(self):
        with self._lock:
            self.errors += 1

    def get_summary(self) -> str:
        with self._lock:
            elapsed = time.time() - self.start_time
            hours, rem = divmod(elapsed, 3600)
            minutes, seconds = divmod(rem, 60)

            summary = [
                f"{Color.BOLD}{Color.CYAN}📊 СТАТИСТИКА РАБОТЫ:{Color.END}",
                f"{Color.GREEN}✓ Обработано файлов:{Color.END} {self.files_processed}",
                f"{Color.BLUE}→ Перемещено:{Color.END} {self.files_moved}",
                f"{Color.YELLOW}⏭ Пропущено:{Color.END} {self.files_skipped}",
                f"{Color.RED}✗ Ошибок:{Color.END} {self.errors}",
                f"{Color.PURPLE}⏱ Время работы:{Color.END} {int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
            ]

            if self.by_extension:
                ext_stats = [f"{LogArt.get_file_emoji(ext)} {ext}: {count}"
                            for ext, count in sorted(self.by_extension.items(), key=lambda x: x[1], reverse=True)[:5]]
                summary.append(f"{Color.CYAN}📈 По типам:{Color.END} " + ", ".join(ext_stats))

            return "\n".join(summary)

STATS = Statistics()

# Low-level file write for logs to avoid recursion
def _write_log_file(file_msg: str):
    try:
        with open(LOGFILE, "a", encoding="utf-8") as f:
            f.write(file_msg + "\n")
    except Exception as e:
        print(f"{Color.RED}⚠ Не могу записать в лог-файл: {e}{Color.END}")

def log(msg: str, level: str = "INFO", show_emoji: bool = True, dont_repeat_stats: bool = False):
    """Safe logging without recursive calls."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    emoji = LogArt.get_random_emoji() if show_emoji else ""

    if level == "SUCCESS":
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {LogStyle.success(msg)}"
    elif level == "INFO":
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {LogStyle.info(msg)}"
    elif level == "WARN":
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {LogStyle.warning(msg)}"
    elif level == "ERROR":
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {LogStyle.error(msg)}"
    elif level == "DEBUG":
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {LogStyle.debug(msg)}"
    elif level == "PROCESSING":
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {LogStyle.processing(msg)}"
    else:
        console_msg = f"{Color.GRAY}[{timestamp}]{Color.END} {emoji} {msg}"

    file_msg = f"[{datetime.now().isoformat()}] [{level}] {msg}"
    _write_log_file(file_msg)
    print(console_msg)

    # Periodic quick summary (no recursive log calls)
    if not dont_repeat_stats and STATS.files_processed > 0 and STATS.files_processed % 10 == 0:
        if random.random() < 0.3:
            summary_msg = f"Обработано уже {STATS.files_processed} файлов! {LogArt.get_random_emoji()}"
            # console
            timestamp2 = datetime.now().strftime("%H:%M:%S")
            print(f"{Color.GRAY}[{timestamp2}]{Color.END} {LogStyle.info(summary_msg)}")
            # file
            _write_log_file(f"[{datetime.now().isoformat()}] [INFO] {summary_msg}")

def log_banner():
    banners = [
        f"""{Color.BOLD}{Color.CYAN}
╔══════════════════════════════════════════╗
║    🚀 COPYPARTY ФОТО-СОРТИРОВЩИК 🚀     ║
║        Автоматическая организация        ║
║              медиа-файлов                ║
╚══════════════════════════════════════════╝{Color.END}
""",
        f"""{Color.BOLD}{Color.PURPLE}
┌──────────────────────────────────────────┐
│   📸 PHOTO ORGANIZER 3000 📸            │
│      для Copyparty                       │
└──────────────────────────────────────────┘{Color.END}
"""
    ]
    print(random.choice(banners))
    print(f"{Color.GRAY}{'='*50}{Color.END}\n")

def log_file_discovery(count: int):
    messages = [
        f"🔍 Обнаружено {count} файлов для обработки",
        f"📂 Найдено {count} файлов в очереди",
        f"🎯 Целей для сортировки: {count}"
    ]
    log(random.choice(messages), "INFO")

def log_file_processing(filename: str, emoji: str = "📄"):
    messages = [
        f"{emoji} Начинаю обработку: {Color.CYAN}{filename}{Color.END}",
        f"{emoji} Анализирую файл: {Color.BLUE}{filename}{Color.END}",
        f"{emoji} В работе: {Color.PURPLE}{filename}{Color.END}"
    ]
    log(random.choice(messages), "PROCESSING")

def log_file_moved(source: str, destination: str, file_emoji: str):
    src_short = Path(source).name
    try:
        dst_short = str(Path(destination).relative_to(PHOTOS_ROOT))
    except Exception:
        dst_short = str(destination)
    styles = [
        LogStyle.file_operation(src_short, dst_short, file_emoji) if hasattr(LogStyle, "file_operation") else f"{file_emoji} {src_short} → {dst_short}",
        f"{file_emoji} {Color.GREEN}Успешно перемещён:{Color.END} {Color.CYAN}{dst_short}{Color.END}"
    ]
    log(random.choice(styles), "SUCCESS")

def log_duplicate_found(filename: str):
    messages = [
        f"🔄 Дубликат обнаружен, удаляю: {Color.YELLOW}{filename}{Color.END}",
        f"⚡ Файл уже существует, пропускаю: {Color.GRAY}{filename}{Color.END}"
    ]
    log(random.choice(messages), "WARN")

def log_error(filename: str, error: str):
    messages = [
        f"💥 Ошибка при обработке {filename}: {error}",
        f"⚠️ Сбой в работе с {filename}: {error}"
    ]
    log(random.choice(messages), "ERROR")

def log_progress(current: int, total: int):
    if total > 0:
        progress = current / total
        bar = ProgressBar.create(progress)
        messages = [
            f"📊 Прогресс: {bar} ({current}/{total})",
            f"⏳ Обработка: {bar}"
        ]
        if current % max(1, total // 10) == 0 or current == total:
            log(random.choice(messages), "INFO")

# ------------------ File stability & metadata ------------------

def is_file_stable(filepath: Path, min_stable_seconds: float = 2.0, max_wait_seconds: float = 1800.0) -> bool:
    """
    Adaptive stability check for large files.
    Waits until file size doesn't change for `adaptive_stable` seconds,
    but not longer than max_wait_seconds.
    Also tries to open file in append mode to detect locks on Windows.
    """
    try:
        if not filepath.exists():
            return False

        total_size = filepath.stat().st_size
        if total_size == 0:
            return False

        size_mb = max(1.0, total_size / (1024.0 * 1024.0))
        adaptive_stable = min(max(min_stable_seconds, 0.2 * size_mb), 60.0)
        adaptive_max_wait = min(max_wait_seconds, max(60.0, size_mb * 2))

        start = time.time()
        last_size = total_size
        stable_since = None

        while True:
            try:
                # Try opening file for append; if file still being written, many writers will block this.
                with open(filepath, "ab"):
                    pass
            except Exception:
                stable_since = None
                log(f"File appears locked (in use): {filepath}", "DEBUG")
                time.sleep(0.5)
                if time.time() - start > adaptive_max_wait:
                    log(f"Timeout waiting for unlocked file: {filepath}", "WARN")
                    return False
                continue

            try:
                cur_size = filepath.stat().st_size
            except Exception as e:
                log(f"Cannot stat file during stability check {filepath}: {e}", "DEBUG")
                return False

            if cur_size != last_size:
                last_size = cur_size
                stable_since = None
                time.sleep(0.5)
                if time.time() - start > adaptive_max_wait:
                    log(f"Timeout waiting for file growth to finish: {filepath}", "WARN")
                    return False
                continue

            if stable_since is None:
                stable_since = time.time()

            if time.time() - stable_since >= adaptive_stable:
                return cur_size > 0

            time.sleep(0.5)
            if time.time() - start > adaptive_max_wait:
                log(f"Timeout waiting for stable file: {filepath}", "WARN")
                return False

    except Exception as e:
        log(f"Cannot check file stability {filepath}: {e}", "WARN")
        return False

def get_exif_datetime(filepath: Path) -> Optional[datetime]:
    try:
        ext = filepath.suffix.lower()
        if ext in ['.heic', '.heif']:
            try:
                from PIL import Image
                import pillow_heif
                pillow_heif.register_heif_opener()
                with Image.open(filepath) as img:
                    exif = img.getexif()
                    if exif:
                        from PIL.ExifTags import TAGS
                        for tag_id, value in exif.items():
                            tag_name = TAGS.get(tag_id, tag_id)
                            if tag_name in ['DateTimeOriginal', 'DateTimeDigitized', 'DateTime'] and value:
                                try:
                                    return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                                except Exception:
                                    continue
            except ImportError:
                log("pillow-heif not installed (optional)", "WARN")
            except Exception as e:
                log(f"Cannot read HEIC metadata {filepath}: {e}", "DEBUG")
            return None

        import piexif
        exif_dict = piexif.load(str(filepath))
        dt_tags = [
            (piexif.ExifIFD.DateTimeOriginal, "DateTimeOriginal"),
            (piexif.ExifIFD.DateTimeDigitized, "DateTimeDigitized"),
            (piexif.ImageIFD.DateTime, "DateTime")
        ]
        for tag_id, tag_name in dt_tags:
            dt = exif_dict.get("Exif", {}).get(tag_id)
            if dt:
                try:
                    return datetime.strptime(dt.decode("utf-8"), "%Y:%m:%d %H:%M:%S")
                except Exception:
                    continue
        return None
    except Exception as e:
        log(f"Cannot read EXIF from {filepath}: {e}", "DEBUG")
        return None

def get_raw_datetime(filepath: Path) -> Optional[datetime]:
    try:
        import exifread
        with open(filepath, 'rb') as f:
            tags = exifread.process_file(f, stop_tag="EXIF DateTimeOriginal", details=False)
            dt_tags = ["EXIF DateTimeOriginal", "EXIF DateTimeDigitized", "Image DateTime"]
            for tag in dt_tags:
                dt_str = str(tags.get(tag, ''))
                if dt_str and dt_str != 'None':
                    for fmt in ["%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            return datetime.strptime(dt_str, fmt)
                        except ValueError:
                            continue
        return None
    except Exception as e:
        log(f"Cannot read RAW metadata from {filepath}: {e}", "DEBUG")
        return None

def get_video_datetime(filepath: Path) -> Optional[datetime]:
    try:
        import ffmpeg
        probe = ffmpeg.probe(str(filepath))
        creation_time = None
        if "format" in probe and "tags" in probe["format"]:
            tags = probe["format"]["tags"]
            creation_time = tags.get("creation_time") or tags.get("creation_date")
        if not creation_time and "streams" in probe:
            for stream in probe["streams"]:
                if "tags" in stream:
                    tags = stream["tags"]
                    creation_time = tags.get("creation_time") or tags.get("creation_date")
                    if creation_time:
                        break
        if creation_time:
            creation_time = creation_time.replace('Z', '+00:00')
            try:
                return datetime.fromisoformat(creation_time)
            except Exception:
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        return datetime.strptime(creation_time.split('.')[0], fmt)
                    except Exception:
                        continue
        return None
    except Exception as e:
        log(f"Cannot read video metadata from {filepath}: {e}", "DEBUG")
        return None

def get_file_datetime(filepath: Path) -> datetime:
    try:
        stat = filepath.stat()
        mtime = datetime.fromtimestamp(stat.st_mtime)
        ctime = datetime.fromtimestamp(stat.st_ctime)
        return min(mtime, ctime)
    except Exception as e:
        log(f"Cannot get file date for {filepath}: {e}", "WARN")
        return datetime.now()

# ------------------ Duplicate detection & robust move ------------------

def file_md5(path: Path, block_size: int = 65536) -> str:
    h = hashlib.md5()
    try:
        with open(path, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                h.update(chunk)
    except Exception as e:
        log(f"Error while computing md5 for {path}: {e}", "DEBUG")
        return ''
    return h.hexdigest()

def is_duplicate(filepath: Path, dest_dir: Path) -> bool:
    try:
        if not dest_dir.exists():
            return False
        dest_file = dest_dir / filepath.name
        if dest_file.exists():
            try:
                if filepath.stat().st_size != dest_file.stat().st_size:
                    return False
                if CHECKSUM_ON_DUP:
                    md1 = file_md5(filepath)
                    md2 = file_md5(dest_file)
                    if md1 and md2 and md1 == md2:
                        return True
                    return False
                else:
                    return True
            except Exception as e:
                log(f"Cannot compare files for duplicate {filepath}: {e}", "DEBUG")
                return False
        return False
    except Exception as e:
        log(f"Cannot check for duplicates {filepath}: {e}", "DEBUG")
        return False

# retry helper
def retry_op(fn, attempts=6, base_delay=0.5, max_delay=5.0, retry_on=(PermissionError, OSError)):
    delay = base_delay
    for attempt in range(1, attempts + 1):
        try:
            return True, fn()
        except Exception as e:
            is_retry = False
            if isinstance(e, retry_on):
                if isinstance(e, OSError):
                    if getattr(e, 'winerror', None) in (32, 5) or getattr(e, 'errno', None) in (errno.EACCES, errno.EPERM):
                        is_retry = True
                else:
                    is_retry = True
            if not is_retry:
                return False, e
            log(f"Retry {attempt}/{attempts} after error: {e}", "WARN")
            if attempt == attempts:
                return False, e
            time.sleep(min(max_delay, delay + random.uniform(0, 0.25)))
            delay *= 2
    return False, RuntimeError("Retry loop ended unexpectedly")

def copy_with_retries(src: Path, dst: Path, attempts: int = RETRY_ATTEMPTS, buffer_size: int = COPY_BUFFER_SIZE):
    def _copy():
        total = src.stat().st_size
        show_progress = total > 50 * 1024 * 1024
        copied = 0
        with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
            while True:
                buf = fsrc.read(buffer_size)
                if not buf:
                    break
                fdst.write(buf)
                copied += len(buf)
                if show_progress:
                    pct = (copied / total)
                    # Use dont_repeat_stats to avoid recursive log
                    log(f"Copy progress {src.name}: {copied}/{total} bytes ({pct:.1%})", "DEBUG", dont_repeat_stats=True)
        shutil.copystat(str(src), str(dst))
        return True

    ok, res = retry_op(_copy, attempts=attempts)
    if not ok:
        log(f"copy_with_retries failed {src} -> {dst}: {res}", "ERROR")
        return False
    return True

def replace_with_retries(tmp: Path, dst: Path, attempts: int = RETRY_ATTEMPTS):
    def _replace():
        os.replace(str(tmp), str(dst))
        return True
    ok, res = retry_op(_replace, attempts=attempts)
    if not ok:
        log(f"replace_with_retries failed {tmp} -> {dst}: {res}", "ERROR")
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception as e:
            log(f"Cannot remove temp file {tmp}: {e}", "DEBUG")
        return False
    return True

def unlink_with_retries(path: Path, attempts: int = RETRY_ATTEMPTS):
    def _unlink():
        path.unlink()
        return True
    ok, res = retry_op(_unlink, attempts=attempts)
    if ok:
        return True
    try:
        locked_dir = WATCH_DIR / "._failed_locked"
        locked_dir.mkdir(parents=True, exist_ok=True)
        new_name = f"{path.stem}_locked_{int(time.time())}{path.suffix}"
        dest = locked_dir / new_name
        try:
            shutil.move(str(path), str(dest))
            log(f"Moved locked source to {dest}", "WARN")
            return True
        except Exception as e:
            log(f"Cannot move locked file {path} to {dest}: {e}", "ERROR")
            return False
    except Exception as e:
        log(f"Cannot handle locked file {path}: {e}", "ERROR")
        return False

def atomic_move(src: Path, dst: Path, dry_run: bool = False) -> bool:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        log(f"[dry-run] would move {src} -> {dst}", "INFO")
        return True

    pid = os.getpid()
    ts = int(time.time() * 1000)
    tmp = dst.with_name(f"{dst.stem}.{pid}.{ts}.tmp{dst.suffix}")

    try:
        if tmp.exists():
            try:
                tmp.unlink()
            except Exception:
                pass
    except Exception:
        pass

    ok = copy_with_retries(src, tmp, attempts=RETRY_ATTEMPTS, buffer_size=COPY_BUFFER_SIZE)
    if not ok:
        log(f"atomic_move: initial copy failed for {src}", "ERROR")
        return False

    ok = replace_with_retries(tmp, dst, attempts=RETRY_ATTEMPTS)
    if not ok:
        log(f"atomic_move: replace failed for {tmp} -> {dst}", "ERROR")
        return False

    if not unlink_with_retries(src, attempts=RETRY_ATTEMPTS):
        log(f"atomic_move: could not delete source {src} after move", "WARN")
    return True

# ------------------ Main sorting logic ------------------

def should_ignore(path: Path) -> bool:
    try:
        if not path.exists():
            return True
        if path.suffix.lower() in IGNORE_EXT:
            return True
        if path.name.startswith(IGNORE_PREFIXES):
            return True
        for part in path.parts:
            if part.lower() in IGNORE_DIRS:
                return True
        # hidden on Windows
        if os.name == 'nt':
            try:
                import ctypes
                FILE_ATTRIBUTE_HIDDEN = 0x2
                attrs = ctypes.windll.kernel32.GetFileAttributesW(str(path))
                if attrs != -1 and (attrs & FILE_ATTRIBUTE_HIDDEN):
                    return True
            except Exception:
                pass
        else:
            if path.name.startswith('.'):
                return True
    except Exception as e:
        log(f"Error in should_ignore for {path}: {e}", "DEBUG")
    return False

def create_unique_filename(dest_dir: Path, filename: str) -> Path:
    dest_file = dest_dir / filename
    if not dest_file.exists():
        return dest_file
    stem = dest_file.stem
    suffix = dest_file.suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{stem}_{timestamp}{suffix}"
    new_file = dest_dir / new_filename
    if not new_file.exists():
        return new_file
    counter = 1
    while True:
        new_filename = f"{stem}_{timestamp}_{counter}{suffix}"
        new_file = dest_dir / new_filename
        if not new_file.exists():
            return new_file
        counter += 1
        if counter > 100:
            return dest_dir / f"{stem}_{int(time.time())}{suffix}"

def sort_file(filepath: Path):
    filepath = filepath.resolve()
    file_key = str(filepath)

    if should_ignore(filepath):
        return

    ext = filepath.suffix.lower()
    if ext not in ALLOWED_EXT:
        log(f"Skipping unsupported file type: {filepath.name} ({ext})", "DEBUG")
        return

    with LOCK:
        if file_key in PROCESSING_FILES:
            log(f"File already queued: {filepath}", "DEBUG")
            return
        current_time = time.time()
        if file_key in FILE_HISTORY and (current_time - FILE_HISTORY[file_key] < 300):
            return
        PROCESSING_FILES.add(file_key)
        FILE_HISTORY[file_key] = current_time
        if len(FILE_HISTORY) > MAX_PROCESSING_HISTORY:
            oldest_keys = sorted(FILE_HISTORY.items(), key=lambda x: x[1])[:100]
            for key, _ in oldest_keys:
                FILE_HISTORY.pop(key, None)

    file_emoji = LogArt.get_file_emoji(ext)
    STATS.add_processed(ext)

    try:
        log_file_processing(filepath.name, file_emoji)

        for attempt in range(MAX_TRIES):
            if STOP_EVENT.is_set():
                return
            if is_file_stable(filepath):
                break
            time.sleep(1)
        else:
            log(f"File not stable after {MAX_TRIES} attempts: {filepath.name}", "WARN")
            STATS.add_error()
            return

        dt = None
        if ext in {'.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.gif', '.bmp', '.tiff'}:
            dt = get_exif_datetime(filepath)
        elif ext in {'.cr2', '.cr3', '.nef', '.arw', '.raf', '.orf', '.rw2', '.dng', '.sr2'}:
            dt = get_raw_datetime(filepath)
        elif ext in {'.mp4', '.mov', '.avi', '.mkv', '.mts', '.m2ts'}:
            dt = get_video_datetime(filepath)

        if not dt:
            dt = get_file_datetime(filepath)
            log(f"Using file date for {filepath.name}: {dt}", "INFO")

        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")

        dest_dir = PHOTOS_ROOT / year / month / day
        dest_dir.mkdir(parents=True, exist_ok=True)

        # Duplicate detection
        if is_duplicate(filepath, dest_dir):
            log_duplicate_found(filepath.name)
            STATS.add_skipped()
            if not DRY_RUN:
                if not unlink_with_retries(filepath):
                    log(f"Could not remove duplicate source: {filepath}", "WARN")
            clean_empty_dirs(filepath.parent)
            return

        dest_file = create_unique_filename(dest_dir, filepath.name)

        moved = atomic_move(filepath, dest_file, dry_run=DRY_RUN)
        if moved:
            STATS.add_moved()
            log_file_moved(str(filepath), str(dest_file), file_emoji)
        else:
            # fallback: try copy_with_retries + unlink_with_retries
            log(f"Atomic move failed for {filepath.name}, trying fallback copy", "WARN")
            ok = copy_with_retries(filepath, dest_file, attempts=RETRY_ATTEMPTS, buffer_size=COPY_BUFFER_SIZE)
            if ok:
                if not DRY_RUN:
                    if not unlink_with_retries(filepath):
                        log(f"Copied but could not delete source {filepath}", "WARN")
                STATS.add_moved()
                log_file_moved(str(filepath), str(dest_file), file_emoji)
            else:
                log_error(filepath.name, "Both atomic_move and fallback copy failed")
                STATS.add_error()

        clean_empty_dirs(filepath.parent)
    except Exception as e:
        log_error(filepath.name, str(e))
        STATS.add_error()
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "DEBUG")
    finally:
        with LOCK:
            PROCESSING_FILES.discard(file_key)

def clean_empty_dirs(start_dir: Path):
    try:
        if not start_dir.exists() or start_dir == WATCH_DIR or not start_dir.is_dir():
            return
        for item in list(start_dir.iterdir()):
            if item.is_dir() and not should_ignore(item):
                clean_empty_dirs(item)
        if start_dir.exists() and start_dir.is_dir():
            try:
                items = list(start_dir.iterdir())
                if not items:
                    start_dir.rmdir()
                    log(f"Removed empty directory: {start_dir}", "INFO")
            except (OSError, PermissionError):
                pass
    except Exception as e:
        log(f"Cannot clean directories {start_dir}: {e}", "DEBUG")

# ------------------ Watchdog handler & initial scan ------------------

class SortingHandler(FileSystemEventHandler):
    def __init__(self, executor):
        self.executor = executor

    def on_created(self, event):
        if not event.is_directory:
            self._schedule_sorting(Path(event.src_path))

    def on_moved(self, event):
        if not event.is_directory:
            # For moved events, use dest_path
            try:
                self._schedule_sorting(Path(event.dest_path))
            except Exception:
                self._schedule_sorting(Path(event.src_path))

    def _schedule_sorting(self, filepath: Path):
        if not filepath.is_file() or should_ignore(filepath):
            return
        time.sleep(WAIT_SEC)
        if STOP_EVENT.is_set():
            return
        self.executor.submit(sort_file, filepath)

def initial_scan():
    log(f"Starting initial scan of {WATCH_DIR}", "INFO")
    file_list = []
    for filepath in WATCH_DIR.rglob("*"):
        if STOP_EVENT.is_set():
            break
        if not filepath.is_file() or should_ignore(filepath):
            continue
        file_list.append(filepath)

    log_file_discovery(len(file_list))

    if file_list:
        log(f"Starting processing {len(file_list)} files...", "INFO")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {executor.submit(sort_file, fp): fp for fp in file_list}
            for i, future in enumerate(as_completed(futures), 1):
                try:
                    future.result()
                    log_progress(i, len(file_list))
                except Exception as e:
                    filepath = futures[future]
                    log_error(filepath.name, str(e))
    log("Initial scan completed", "SUCCESS")
    print()

# ------------------ Utilities & main ------------------

def signal_handler(signum, frame):
    log("Shutdown signal received", "WARN")
    STOP_EVENT.set()

def check_dependencies():
    try:
        import watchdog  # noqa: F401
        import piexif   # noqa: F401
        import exifread # noqa: F401
        import ffmpeg   # noqa: F401
        from PIL import Image  # noqa: F401
        log("All main dependencies available", "SUCCESS")
        try:
            import pillow_heif  # noqa: F401
            log("pillow-heif available (HEIC support)", "INFO")
        except ImportError:
            log("pillow-heif not installed (optional)", "WARN")
        return True
    except ImportError as e:
        log(f"Missing dependency: {e}", "ERROR")
        log("Install: pip install watchdog piexif exifread ffmpeg-python Pillow pillow-heif", "INFO")
        return False

def parse_args():
    parser = argparse.ArgumentParser(description="Copyparty Auto Sorter (styled & robust)")
    parser.add_argument("--watch", type=str, default=str(DEFAULT_WATCH_DIR), help="Watch directory")
    parser.add_argument("--target", type=str, default=str(DEFAULT_PHOTOS_ROOT), help="Target root for sorted photos")
    parser.add_argument("--log", type=str, default=str(DEFAULT_LOGFILE), help="Log file")
    parser.add_argument("--workers", type=int, default=MAX_WORKERS, help="Max worker threads")
    parser.add_argument("--dry-run", action="store_true", help="Do not modify files, just log actions")
    parser.add_argument("--no-checksum-dups", action="store_true", help="Do not compute md5 to confirm duplicates")
    parser.add_argument("--buffer-size-mb", type=int, default=int(COPY_BUFFER_SIZE / (1024 * 1024)), help="Copy buffer size in MB")
    return parser.parse_args()

def main():
    global WATCH_DIR, PHOTOS_ROOT, LOGFILE, DRY_RUN, MAX_WORKERS, CHECKSUM_ON_DUP, COPY_BUFFER_SIZE

    args = parse_args()
    WATCH_DIR = Path(args.watch)
    PHOTOS_ROOT = Path(args.target)
    LOGFILE = Path(args.log)
    DRY_RUN = args.dry_run
    MAX_WORKERS = args.workers
    CHECKSUM_ON_DUP = not args.no_checksum_dups
    COPY_BUFFER_SIZE = max(1024 * 1024, args.buffer_size_mb * 1024 * 1024)

    log_banner()
    log(f"{Color.BOLD}Configuration:{Color.END}", "INFO")
    log(f"  Watch: {Color.CYAN}{WATCH_DIR}{Color.END}", "INFO")
    log(f"  Target: {Color.GREEN}{PHOTOS_ROOT}{Color.END}", "INFO")
    log(f"  Log file: {Color.YELLOW}{LOGFILE}{Color.END}", "INFO")
    log(f"  Workers: {Color.YELLOW}{MAX_WORKERS}{Color.END}", "INFO")
    log(f"  Dry-run: {Color.YELLOW}{DRY_RUN}{Color.END}", "INFO")
    print()

    if not check_dependencies():
        return

    if not WATCH_DIR.exists():
        log(f"Watch directory does not exist: {WATCH_DIR}", "ERROR")
        return

    if not PHOTOS_ROOT.exists():
        try:
            if not DRY_RUN:
                PHOTOS_ROOT.mkdir(parents=True, exist_ok=True)
            log(f"Created target directory: {PHOTOS_ROOT}", "SUCCESS")
        except Exception as e:
            log(f"Cannot create target directory: {e}", "ERROR")
            return

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    if not STOP_EVENT.is_set():
        initial_scan()

    if not STOP_EVENT.is_set():
        log(f"Starting filesystem observer on {WATCH_DIR}", "INFO")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            event_handler = SortingHandler(executor)
            observer = Observer()
            try:
                observer.schedule(event_handler, str(WATCH_DIR), recursive=True)
                observer.start()
                log("Observer started", "SUCCESS")
            except Exception as e:
                log(f"Cannot start observer: {e}", "ERROR")
                return

            last_stats_time = time.time()
            try:
                while not STOP_EVENT.is_set():
                    time.sleep(1)
                    if time.time() - last_stats_time > 300:
                        print()
                        log(LogStyle.header("PERIODIC STATS"), "INFO")
                        print(STATS.get_summary())
                        print()
                        last_stats_time = time.time()
            except KeyboardInterrupt:
                log("Keyboard interrupt received", "WARN")
                STOP_EVENT.set()
            except Exception as e:
                log(f"Observer error: {e}", "ERROR")
            observer.stop()
            observer.join()

    print()
    log(LogStyle.header("SHUTDOWN"), "INFO")
    print(STATS.get_summary())

if __name__ == "__main__":
    main()
