#v1.4.6
import os
import time
import shutil
import signal
import hashlib
from datetime import datetime
from typing import Optional, Set, Dict
from pathlib import Path
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileCreatedEvent, FileMovedEvent

# Конфигурация3

WATCH_DIR = Path(r"C:\uploads")#Вход
PHOTOS_ROOT = Path(r"J:\Photos")#Выходы
WAIT_SEC = 5.0
MAX_TRIES = 10
LOGFILE = Path(r"J:\DESKROP\COPYPARTY_Host\uploader\copyparty_sorter.log") #Логи    
MAX_WORKERS = 4
MAX_PROCESSING_HISTORY = 1000

# Поддерживаемые расширения
ALLOWED_EXT = {
    '.jpg', '.jpeg', '.png', '.cr2', '.cr3', '.nef', '.arw',
    '.mp4', '.mov', '.avi', '.mkv', '.mts', '.m2ts',
    '.webp', '.heic', '.heif',
    '.raf', '.orf', '.rw2', '.dng', '.sr2', '.gif', '.bmp', '.tiff'
}

# Игнорируемые директории и файлы
IGNORE_DIRS = {'.hist', '.tmp', 'temp', 'tmp', 'cache', 'thumbnail', 'thumb'}
IGNORE_PREFIXES = ('.', '~', 'Thumbs.db')
IGNORE_EXT = {'.tmp', '.temp', '.crdownload', '.part', '.lnk'}

# Глобальные переменные
PROCESSING_FILES: Set[str] = set()
FILE_HISTORY: Dict[str, float] = {}
LOCK = threading.Lock()
STOP_EVENT = threading.Event()

def log(msg: str, level: str = "INFO"):
    """Улучшенное логирование"""
    timestamp = datetime.now().isoformat(sep=' ', timespec='seconds')
    log_msg = f"[{timestamp}] [{level}] {msg}"
    
    try:
        with open(LOGFILE, "a", encoding="utf-8") as f:
            f.write(log_msg + "\n")
    except Exception as e:
        print(f"[WARN] Cannot write to log file: {e}")
    
    # Вывод в консоль
    if level == "ERROR":
        print(f"\033[91m{log_msg}\033[0m")
    elif level == "WARN":
        print(f"\033[93m{log_msg}\033[0m")
    elif level == "INFO":
        print(f"\033[92m{log_msg}\033[0m")
    else:
        print(log_msg)

def should_ignore(path: Path) -> bool:
    """Проверка, нужно ли игнорировать файл/папку"""
    if not path.exists():
        return True
    
    # Игнорировать по расширению
    if path.suffix.lower() in IGNORE_EXT:
        return True
    
    # Игнорировать по префиксу имени
    if path.name.startswith(IGNORE_PREFIXES):
        return True
    
    # Игнорировать определенные директории в пути
    for part in path.parts:
        if part.lower() in IGNORE_DIRS:
            return True
    
    # Игнорировать скрытые файлы/папки
    try:
        if os.name == 'nt':  # Windows
            import ctypes
            FILE_ATTRIBUTE_HIDDEN = 0x2
            if ctypes.windll.kernel32.GetFileAttributesW(str(path)) & FILE_ATTRIBUTE_HIDDEN:
                return True
        else:  # Linux/Mac
            if path.name.startswith('.'):
                return True
    except:
        pass
    
    return False

def is_file_stable(filepath: Path, check_interval: float = 1.0) -> bool:
    """Проверка, что файл завершил копирование"""
    try:
        if not filepath.exists():
            return False
        
        # Первая проверка размера
        size1 = filepath.stat().st_size
        if size1 == 0:  # Пустой файл
            return False
        
        time.sleep(check_interval)
        
        # Вторая проверка размера
        size2 = filepath.stat().st_size
        if size1 != size2:
            return False
        
        # Третья проверка через секунду
        time.sleep(1)
        size3 = filepath.stat().st_size
        if size2 != size3:
            return False
        
        # Попытка открыть файл
        with open(filepath, 'rb') as f:
            f.read(1)
        
        return True
    except (OSError, IOError) as e:
        log(f"Cannot check file stability {filepath}: {e}", "WARN")
        return False

def get_exif_datetime(filepath: Path) -> Optional[datetime]:
    """Получить дату из EXIF для различных форматов"""
    try:
        ext = filepath.suffix.lower()
        
        # Для HEIC/HEIF используем pillow-heif
        if ext in ['.heic', '.heif']:
            try:
                from PIL import Image
                import pillow_heif
                pillow_heif.register_heif_opener()
                
                with Image.open(filepath) as img:
                    exif = img.getexif()
                    if exif:
                        # EXIF тег для DateTimeOriginal
                        from PIL.ExifTags import TAGS
                        for tag_id, value in exif.items():
                            tag_name = TAGS.get(tag_id, tag_id)
                            if tag_name in ['DateTimeOriginal', 'DateTimeDigitized', 'DateTime']:
                                if value:
                                    try:
                                        return datetime.strptime(value, "%Y:%m:%d %H:%M:%S")
                                    except ValueError:
                                        continue
            except ImportError:
                log(f"pillow-heif not installed for HEIC support", "WARN")
            except Exception as e:
                log(f"Cannot read HEIC metadata {filepath}: {e}", "DEBUG")
            return None
        
        # Для остальных форматов используем piexif
        import piexif
        
        exif_dict = piexif.load(str(filepath))
        
        # Пробуем разные теги EXIF
        dt_tags = [
            (piexif.ExifIFD.DateTimeOriginal, "DateTimeOriginal"),
            (piexif.ExifIFD.DateTimeDigitized, "DateTimeDigitized"),
            (piexif.ImageIFD.DateTime, "DateTime")
        ]
        
        for tag_id, tag_name in dt_tags:
            dt = exif_dict["Exif"].get(tag_id) if tag_id in exif_dict["Exif"] else None
            if dt:
                try:
                    return datetime.strptime(dt.decode("utf-8"), "%Y:%m:%d %H:%M:%S")
                except (ValueError, UnicodeDecodeError) as e:
                    log(f"Invalid date format in {tag_name} for {filepath}: {e}", "DEBUG")
                    continue
        
        return None
    except Exception as e:
        log(f"Cannot read EXIF from {filepath}: {e}", "DEBUG")
        return None

def get_raw_datetime(filepath: Path) -> Optional[datetime]:
    """Получить дату из RAW файлов"""
    try:
        import exifread
        with open(filepath, 'rb') as f:
            tags = exifread.process_file(f, stop_tag="EXIF DateTimeOriginal", details=False)
            
            dt_tags = ["EXIF DateTimeOriginal", "EXIF DateTimeDigitized", "Image DateTime"]
            
            for tag in dt_tags:
                dt_str = str(tags.get(tag, ''))
                if dt_str and dt_str != 'None':
                    try:
                        # Разные форматы даты в EXIF
                        for fmt in ["%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                            try:
                                return datetime.strptime(dt_str, fmt)
                            except ValueError:
                                continue
                    except Exception as e:
                        log(f"Invalid date in {tag} for {filepath}: {e}", "DEBUG")
        
        return None
    except Exception as e:
        log(f"Cannot read RAW metadata from {filepath}: {e}", "DEBUG")
        return None

def get_video_datetime(filepath: Path) -> Optional[datetime]:
    """Получить дату из видео файлов"""
    try:
        import ffmpeg
        
        probe = ffmpeg.probe(str(filepath))
        
        # Пробуем получить дату из метаданных
        creation_time = None
        
        # Из тегов формата
        if "format" in probe and "tags" in probe["format"]:
            tags = probe["format"]["tags"]
            creation_time = tags.get("creation_time") or tags.get("creation_date")
        
        # Из тегов потоков
        if not creation_time and "streams" in probe:
            for stream in probe["streams"]:
                if "tags" in stream:
                    tags = stream["tags"]
                    creation_time = tags.get("creation_time") or tags.get("creation_date")
                    if creation_time:
                        break
        
        if creation_time:
            # Преобразуем разные форматы даты
            creation_time = creation_time.replace('Z', '+00:00')
            try:
                return datetime.fromisoformat(creation_time)
            except ValueError:
                # Пробуем другие форматы
                for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"]:
                    try:
                        return datetime.strptime(creation_time.split('.')[0], fmt)
                    except ValueError:
                        continue
        
        return None
    except Exception as e:
        log(f"Cannot read video metadata from {filepath}: {e}", "DEBUG")
        return None

def get_file_datetime(filepath: Path) -> datetime:
    """Получить дату из системных атрибутов файла"""
    try:
        stat = filepath.stat()
        
        # Предпочтительная дата - дата изменения содержимого
        mtime = datetime.fromtimestamp(stat.st_mtime)
        ctime = datetime.fromtimestamp(stat.st_ctime)
        
        # Используем более раннюю дату
        return min(mtime, ctime)
    except Exception as e:
        log(f"Cannot get file date for {filepath}: {e}", "WARN")
        return datetime.now()

def is_duplicate(filepath: Path, dest_dir: Path) -> bool:
    """Проверить, есть ли уже такой файл в папке назначения"""
    try:
        if not dest_dir.exists():
            return False
        
        # Простая проверка по имени
        dest_file = dest_dir / filepath.name
        if dest_file.exists():
            # Проверка размера
            if filepath.stat().st_size == dest_file.stat().st_size:
                return True
        
        return False
    except Exception as e:
        log(f"Cannot check for duplicates {filepath}: {e}", "DEBUG")
        return False

def create_unique_filename(dest_dir: Path, filename: str) -> Path:
    """Создать уникальное имя файла"""
    dest_file = dest_dir / filename
    
    if not dest_file.exists():
        return dest_file
    
    # Разделяем имя и расширение
    stem = dest_file.stem
    suffix = dest_file.suffix
    
    # Добавляем дату/время если файл уже существует
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    new_filename = f"{stem}_{timestamp}{suffix}"
    new_file = dest_dir / new_filename
    
    if not new_file.exists():
        return new_file
    
    # Если и с таймстемпом существует, добавляем число
    counter = 1
    while True:
        new_filename = f"{stem}_{timestamp}_{counter}{suffix}"
        new_file = dest_dir / new_filename
        if not new_file.exists():
            return new_file
        counter += 1
        if counter > 100:
            # На всякий случай ограничим
            return dest_dir / f"{stem}_{int(time.time())}{suffix}"

def sort_file(filepath: Path):
    """Основная функция сортировки файла"""
    filepath = filepath.resolve()
    file_key = str(filepath)
    
    # Проверка на игнорирование
    if should_ignore(filepath):
        return
    
    # Проверка расширения
    ext = filepath.suffix.lower()
    if ext not in ALLOWED_EXT:
        log(f"Skipping unsupported file type: {filepath.name} ({ext})", "DEBUG")
        return
    
    # Блокировка для проверки обработки
    with LOCK:
        # Проверяем, не обрабатывается ли уже файл
        if file_key in PROCESSING_FILES:
            log(f"File already being processed: {filepath.name}", "DEBUG")
            return
        
        # Проверяем историю обработки
        current_time = time.time()
        if file_key in FILE_HISTORY:
            if current_time - FILE_HISTORY[file_key] < 300:  # 5 минут
                return
        
        # Добавляем в обработку
        PROCESSING_FILES.add(file_key)
        FILE_HISTORY[file_key] = current_time
        
        # Очищаем старую историю
        if len(FILE_HISTORY) > MAX_PROCESSING_HISTORY:
            # Удаляем самые старые записи
            oldest_keys = sorted(FILE_HISTORY.items(), key=lambda x: x[1])[:100]
            for key, _ in oldest_keys:
                FILE_HISTORY.pop(key, None)
    
    try:
        log(f"Processing: {filepath.name}", "INFO")
        
        # Ждем стабилизации файла
        for attempt in range(MAX_TRIES):
            if STOP_EVENT.is_set():
                return
            if is_file_stable(filepath, check_interval=0.5):
                break
            time.sleep(1)
        else:
            log(f"File not stable after {MAX_TRIES} attempts: {filepath.name}", "WARN")
            return
        
        # Получаем дату создания
        dt = None
        
        # Для фото и изображений
        if ext in {'.jpg', '.jpeg', '.png', '.webp', '.heic', '.heif', '.gif', '.bmp', '.tiff'}:
            dt = get_exif_datetime(filepath)
        
        # Для RAW файлов
        elif ext in {'.cr2', '.cr3', '.nef', '.arw', '.raf', '.orf', '.rw2', '.dng', '.sr2'}:
            dt = get_raw_datetime(filepath)
        
        # Для видео
        elif ext in {'.mp4', '.mov', '.avi', '.mkv', '.mts', '.m2ts'}:
            dt = get_video_datetime(filepath)
        
        # Если не получили дату из метаданных
        if not dt:
            dt = get_file_datetime(filepath)
            log(f"Using file date for {filepath.name}: {dt}", "INFO")
        
        # Создаем структуру папок
        year = dt.strftime("%Y")
        month = dt.strftime("%m")
        day = dt.strftime("%d")
        
        dest_dir = PHOTOS_ROOT / year / month / day
        dest_dir.mkdir(parents=True, exist_ok=True)
        
        # Проверяем дубликаты
        if is_duplicate(filepath, dest_dir):
            log(f"Duplicate found, deleting source: {filepath.name}", "INFO")
            try:
                filepath.unlink()
            except Exception as e:
                log(f"Cannot delete duplicate {filepath}: {e}", "ERROR")
            
            # Очищаем пустые папки
            clean_empty_dirs(filepath.parent)
            return
        
        # Создаем уникальное имя
        dest_file = create_unique_filename(dest_dir, filepath.name)
        
        # Перемещаем файл
        try:
            shutil.move(str(filepath), str(dest_file))
            log(f"Moved: {filepath.name} -> {dest_file.relative_to(PHOTOS_ROOT)}", "INFO")
        except Exception as e:
            log(f"Cannot move file {filepath}: {e}", "ERROR")
            # Пробуем скопировать
            try:
                shutil.copy2(str(filepath), str(dest_file))
                filepath.unlink()
                log(f"Copied and deleted: {filepath.name}", "INFO")
            except Exception as e2:
                log(f"Cannot copy file {filepath}: {e2}", "ERROR")
        
        # Очищаем пустые папки
        clean_empty_dirs(filepath.parent)
        
    except Exception as e:
        log(f"Error processing {filepath.name}: {e}", "ERROR")
        import traceback
        log(f"Traceback: {traceback.format_exc()}", "DEBUG")
    finally:
        # Удаляем из списка обработки
        with LOCK:
            PROCESSING_FILES.discard(file_key)

def clean_empty_dirs(start_dir: Path):
    """Рекурсивно удаляет пустые директории"""
    try:
        if not start_dir.exists() or start_dir == WATCH_DIR or not start_dir.is_dir():
            return
        
        # Сначала очищаем поддиректории
        for item in list(start_dir.iterdir()):
            if item.is_dir() and not should_ignore(item):
                clean_empty_dirs(item)
        
        # Пытаемся удалить текущую директорию если она пуста
        if start_dir.exists() and start_dir.is_dir():
            try:
                items = list(start_dir.iterdir())
                if not items:
                    start_dir.rmdir()
                    log(f"Removed empty directory: {start_dir}", "INFO")
            except (OSError, PermissionError) as e:
                pass
    except Exception as e:
        log(f"Cannot clean directories {start_dir}: {e}", "DEBUG")

class SortingHandler(FileSystemEventHandler):
    """Обработчик событий файловой системы"""
    
    def __init__(self, executor):
        self.executor = executor
    
    def on_created(self, event):
        if not event.is_directory:
            self._schedule_sorting(Path(event.src_path))
    
    def on_moved(self, event):
        if not event.is_directory:
            self._schedule_sorting(Path(event.dest_path))
    
    def _schedule_sorting(self, filepath: Path):
        """Запланировать сортировку файла"""
        if not filepath.is_file() or should_ignore(filepath):
            return
        
        # Ждем перед началом обработки
        time.sleep(WAIT_SEC)
        
        if STOP_EVENT.is_set():
            return
        
        # Запускаем в пуле потоков
        self.executor.submit(sort_file, filepath)

def initial_scan():
    """Начальное сканирование директории"""
    log(f"Starting initial scan of {WATCH_DIR}", "INFO")
    
    files_found = 0
    files_processed = 0
    
    # Получаем список файлов
    file_list = []
    for filepath in WATCH_DIR.rglob("*"):
        if STOP_EVENT.is_set():
            break
        
        if not filepath.is_file() or should_ignore(filepath):
            continue
        
        files_found += 1
        file_list.append(filepath)
    
    log(f"Found {files_found} files to process", "INFO")
    
    # Обрабатываем файлы параллельно
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(sort_file, fp): fp for fp in file_list}
        
        for future in as_completed(futures):
            try:
                future.result()
                files_processed += 1
            except Exception as e:
                filepath = futures[future]
                log(f"Error processing {filepath}: {e}", "ERROR")
    
    log(f"Initial scan completed. Processed {files_processed}/{files_found} files.", "INFO")

def signal_handler(signum, frame):
    """Обработчик сигналов для корректного завершения"""
    log("Shutdown signal received", "INFO")
    STOP_EVENT.set()

def check_dependencies():
    """Проверка необходимых зависимостей"""
    try:
        import watchdog
        import piexif
        import exifread
        import ffmpeg
        from PIL import Image
        
        # Проверяем pillow-heif для HEIC
        try:
            import pillow_heif
            log("pillow-heif available for HEIC/HEIF support", "INFO")
        except ImportError:
            log("pillow-heif not installed (optional for HEIC support)", "WARN")
        
        return True
    except ImportError as e:
        log(f"Missing dependency: {e}", "ERROR")
        log("Install with: pip install watchdog piexif exifread ffmpeg-python Pillow pillow-heif", "ERROR")
        return False

def main():
    """Основная функция"""
    log("=" * 60, "INFO")
    log("Photo Sorter started", "INFO")
    log(f"Watch directory: {WATCH_DIR}", "INFO")
    log(f"Target directory: {PHOTOS_ROOT}", "INFO")
    log("=" * 60, "INFO")
    
    # Проверка зависимостей
    if not check_dependencies():
        return
    
    # Проверка директорий
    if not WATCH_DIR.exists():
        log(f"ERROR: Watch directory does not exist: {WATCH_DIR}", "ERROR")
        return
    
    if not PHOTOS_ROOT.exists():
        try:
            PHOTOS_ROOT.mkdir(parents=True, exist_ok=True)
            log(f"Created target directory: {PHOTOS_ROOT}", "INFO")
        except Exception as e:
            log(f"ERROR: Cannot create target directory: {e}", "ERROR")
            return
    
    # Настройка обработчиков сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Начальное сканирование
    if not STOP_EVENT.is_set():
        initial_scan()
    
    # Запуск наблюдателя
    if not STOP_EVENT.is_set():
        log("Starting file system observer", "INFO")
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            event_handler = SortingHandler(executor)
            observer = Observer()
            
            try:
                observer.schedule(event_handler, str(WATCH_DIR), recursive=True)
                observer.start()
            except Exception as e:
                log(f"Cannot start observer: {e}", "ERROR")
                return
            
            try:
                while not STOP_EVENT.is_set():
                    time.sleep(1)
            except KeyboardInterrupt:
                log("Keyboard interrupt received", "INFO")
                STOP_EVENT.set()
            except Exception as e:
                log(f"Observer error: {e}", "ERROR")
            
            # Корректное завершение
            observer.stop()
            observer.join()
    
    # Завершение работы
    log("Shutting down...", "INFO")
    
    # Даем время на завершение операций
    for _ in range(10):  # 10 секунд на завершение
        with LOCK:
            if not PROCESSING_FILES:
                break
        log(f"Waiting for {len(PROCESSING_FILES)} files to finish...", "INFO")
        time.sleep(1)
    
    log("Photo Sorter stopped", "INFO")

if __name__ == "__main__":
    main()
