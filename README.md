# Автоматический сортировщик фото/видео файлов по годам/месяцам/дням для copyparty (возможно не только для него)

## Установка
1 установите файлы из [uploader](https://github.com/DeniProtoshark/Copyparty-Auto-Sort/tree/Beta-RU/uploader).
2 Откройте файл .py и настройте 
* ```DEFAULT_WATCH_DIR``` = куда вы скидываете файлы (мусорка)
* ```DEFAULT_PHOTOS_ROOT``` = где будет формироватся иерархия в виде ГОД>МЕСЯЦ>ДЕНЬ
* ```DEFAULT_LOGFILE``` = логично что это логи (можно офнуть)
* ```WAIT_SEC``` = пауза перед началом обработки файла после события (даёт время на дозапись/освобождение)
* ```MAX_TRIES``` = сколько раз проверять стабильность файла перед отказом (вместе с WAIT_SEC задаёт задержку)
* ```MAX_WORKERS``` = число параллельных обработок (потоков)
* ```MAX_PROCESSING_HISTORY``` = длина истории обработанных файлов (чтобы не повторять)
* ```COPY_BUFFER_SIZE``` = размер буфера при копировании (в байтах; 8 MB = 810241024)
* ```RETRY_ATTEMPTS``` = уже не ебу что это, но вроде это количество попыток повтора при ошибках

## Зависимости
 * watchdog
 * piexif
 * exifread
 * ffmpeg-python
 * Pillow
 * pillow-heif

Установка зависимостей с помощью
  * ``` pip install watchdog piexif exifread ffmpeg-python Pillow pillow-heif ```          
или
  * ``` pip install -r requirements.txt ``` [уже всё прописано в requirements.txt]
  
<img width="557" height="315" alt="image" src="https://github.com/user-attachments/assets/32203147-de97-4140-a689-c01e0bc7b8cb" />
<img width="557" height="315" alt="image" src="https://github.com/user-attachments/assets/81af89db-c6aa-49fe-a39d-446d064d86ef" />
<img width="557" height="315" alt="image" src="https://github.com/user-attachments/assets/77882ccf-d054-4789-be7b-7b66bbeab67e" />
