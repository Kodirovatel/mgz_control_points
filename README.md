# Загрузчик из МГЗ с обработкой файлов #

Программа забирает файлы из реестра задач по заданным фильтрам и форматирует их, также обрабатывает файл `Состояние объектов.xlsx`.

## Описание программы ##

Перед запуском **необходимо положить файл Состояние объектов.xlsx в папку 2nd_file и очистить папки download и result**.
При запуске можно указать даты для того чтобы пофлиять на фильтры при выгрузке из реестра задач, или оставить всё по умолчанию. Увидите сообщения:  
*Введите дату для даты начала (ДД.ММ.ГГГГ) или Enter для сегодня:*
*Введите дату для даты окончания (ДД.ММ.ГГГГ) или Enter для вчера:*
Далее ввести логин и пароль для авторизации в СУДИР.
Файлы будут загружены в папку **Download** и их можно потом использовать для проверки, после преобразования в папке `result` появятся файлы `начало.xlsx` и `окончание.xlsx`.
После будет преобразован файл `Состояние объектов.xlsx`, результат также появится в **Download**.


## Клонирование репозитория ##

### Требования ###
- Git
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (менеджер пакетов и виртуальных окружений)

### Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/Kodirovatel/mgz_control_points
cd mgz_control_points
```
2. Установите uv (если ещё не установлен):

**Windows:** 
```powershell 
powershell  -c "irm https://astral.sh/uv/install.ps1 | iex"
```
**Linux/macOS:** 
```bash
 curl -LsSf https://astral.sh/uv/install.sh | sh
 ```

3. Установите зависимости:
`uv sync`
4. Запустите программу:
`uv run python mgz_control_points`


## Создание standalone приложения ##
С установленным uv в терминале заходите в папку со скриптами и выполняете команду:   
`uv run pyinstaller --onefile --console --exclude-module IPython --exclude-module jupyter --exclude-module notebook --exclude-module matplotlib mgz_control_points.py`  


В созданной папке mgz_control_points.dist создайте папки download, 2nd_file и result, разместите файл `Состояние объектов.xlsx`,затем запускайте mgz_control_points.exe