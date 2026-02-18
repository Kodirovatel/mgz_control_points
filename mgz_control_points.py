import requests
from bs4 import BeautifulSoup
from datetime import datetime, date, timedelta
import json
import time
import getpass
import os
from pathlib import Path
import pandas as pd

class MgzClient:
    """Клиент для работы с mgz.mos.ru"""

    # конфиги фильтров для скачивания excel файлов 
    FILTER_CONFIGS = {
        "end": {
            "entity_attribute": "PlanEndDate",
            "operation": 2,
            "sub_node_attribute": "FactEndDate",
            "sub_node_row_id": "ext-1398",
            "description": "по окончанию"
        },
        "start": {
            "entity_attribute": "PlanBeginDate",
            "operation": 2,
            "sub_node_attribute": "FactBeginDate",
            "sub_node_row_id": "ext-1399",
            "description": "по началу"
        }
    }
    
    # Колонки для экспорта
    EXPORT_COLUMNS = [
        {'Name': 'ParentWorkKindName', 'Header': 'Главная работа', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 7.29, 'FilterVisibleText': ''},
        {'Name': 'State', 'Header': 'Статус', 'Xtype': 'easstatecolumn', 'Hidden': False, 'WidthInReport': 1.45, 'ValueProperty': 'Name', 'FilterVisibleText': ''},
        {'Name': 'OrderNumberPath', 'Header': 'Номер задачи', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 6.66, 'FilterVisibleText': ''},
        {'Name': 'WorkKindName', 'Header': 'Задача', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 6.66, 'FilterVisibleText': ''},
        {'Name': 'IsAgreedExecutor', 'Header': 'Согласовано исполнителем', 'Xtype': 'gridcolumn', 'Hidden': False, 'WidthInReport': 4, 'EnumDictionary': [[True, 'Да'], [False, 'Нет']], 'FilterVisibleText': ''},
        {'Name': 'AgreedOperator', 'Header': 'Согласовавший оператор', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 4, 'FilterVisibleText': ''},
        {'Name': 'DateOfAgreement', 'Header': 'Дата согласования', 'Xtype': 'easgriddatecolumn', 'Hidden': False, 'WidthInReport': 5, 'FilterVisibleText': ''},
        {'Name': 'ObjectCode', 'Header': 'Код ДС', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 5.62, 'FilterVisibleText': ''},
        {'Name': 'ObjectWorkName', 'Header': 'Объект', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 4.79, 'FilterVisibleText': ''},
        {'Name': 'PrefectureName', 'Header': 'Префектура', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'FilterVisibleText': ''},
        {'Name': 'StatusCurrentDraftDecisionStart', 'Header': 'Состояние текущего предлагаемого решения (начало)', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 5, 'FilterVisibleText': ''},
        {'Name': 'TransfersCountStart', 'Header': 'Кол-во переносов начала задачи', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 4.79},
        {'Name': 'NumberDraftDecisionsStart', 'Header': 'Кол-во предложенных решений (начало)', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 4.79},
        {'Name': 'IsDraftDecisionsStart', 'Header': 'нет заголовка', 'Xtype': 'easgridactionscolumn', 'Hidden': False},
        {'Name': 'StatusCurrentDraftDecision', 'Header': 'Состояние текущего предлагаемого решения (окончание)', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 5, 'FilterVisibleText': ''},
        {'Name': 'TransfersCount', 'Header': 'Кол-во переносов окончания задачи', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 4.79},
        {'Name': 'NumberDraftDecisions', 'Header': 'Кол-во предложенных решений (окончание)', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 4.79},
        {'Name': 'IsDraftDecisions', 'Header': 'нет заголовка', 'Xtype': 'easgridactionscolumn', 'Hidden': False},
        {'Name': 'PlanBeginDate', 'Header': 'План. начало', 'Xtype': 'easgriddatecolumn', 'Hidden': False, 'WidthInReport': 5},
        {'Name': 'PlanEndDate', 'Header': 'План. окончание', 'Xtype': 'easgriddatecolumn', 'Hidden': False, 'WidthInReport': 5},
        {'Name': 'PlanDaysCount', 'Header': 'План. дней', 'Xtype': 'easwraptextcolumn', 'Hidden': False},
        {'Name': 'FactBeginDate', 'Header': 'Факт. начало', 'Xtype': 'easgriddatecolumn', 'Hidden': False},
        {'Name': 'FactEndDate', 'Header': 'Факт. окончание', 'Xtype': 'easgriddatecolumn', 'Hidden': False},
        {'Name': 'FactDaysCount', 'Header': 'Факт. дней', 'Xtype': 'easwraptextcolumn', 'Hidden': False},
        {'Name': 'FactDaysLength', 'Header': 'Смещение', 'Xtype': 'gridcolumn', 'Hidden': False},
        {'Name': 'CompletePercent', 'Header': 'Процент', 'Xtype': 'easwraptextcolumn', 'Hidden': False},
        {'Name': 'ApprovedBeginDate', 'Header': 'Утв. начало', 'Xtype': 'easgriddatecolumn', 'Hidden': False, 'WidthInReport': 5},
        {'Name': 'ApprovedEndDate', 'Header': 'Утв. окончание', 'Xtype': 'easgriddatecolumn', 'Hidden': False, 'WidthInReport': 5},
        {'Name': 'ApprovedDaysCount', 'Header': 'Утв. дней', 'Xtype': 'easwraptextcolumn', 'Hidden': False},
        {'Name': 'ApprovedOffsetBegin', 'Header': 'Смещение начала по утв.', 'Xtype': 'easwraptextcolumn', 'Hidden': False},
        {'Name': 'ApprovedOffsetEnd', 'Header': 'Смещение окончания по утв.', 'Xtype': 'easwraptextcolumn', 'Hidden': False},
        {'Name': 'ApprovedDate', 'Header': 'Дата утверждения', 'Xtype': 'easgriddatecolumn', 'Hidden': False, 'WidthInReport': 5},
        {'Name': 'Developer', 'Header': 'Застройщик', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'FilterVisibleText': ''},
        {'Name': 'Deputy', 'Header': 'Заместитель', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'FilterVisibleText': ''},
        {'Name': 'CompletePercentActualDate', 'Header': 'Актуально', 'Xtype': 'easgriddatecolumn', 'Hidden': False},
        {'Name': 'CuratorName', 'Header': 'Руководитель проекта', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'FilterVisibleText': ''},
        {'Name': 'OwnerName', 'Header': 'Ответственный', 'Xtype': 'gridcolumn', 'Hidden': False, 'WidthInReport': 5, 'ValueStringFormat': {'Text': '{0} ({1})', 'ColumnNames': ['OwnerName', 'OwnerDepartmentName']}, 'FilterVisibleText': ''},
        {'Name': 'OwnerSubdivisionName', 'Header': 'Отдел ответственного', 'Xtype': 'easwraptextcolumn', 'Hidden': False, 'WidthInReport': 5},
        {'Name': 'LastComment', 'Header': 'Последняя проблематика', 'Xtype': 'easwraptextcolumn', 'Hidden': False}
    ]

    def __init__(self, login, password):
        self.login = login
        self.password = password
        self.session = requests.Session()
        self.oauth_token = None
        self.base_url = "https://mgz.mos.ru"
        self.download_dir = Path.cwd() / 'download'
        self.result_dir = Path.cwd() / 'result'

    def _build_filter(self, filter_date: datetime, filter_type: str) -> dict:
        """
        Построить объект фильтра
        
        Args:
            filter_date: Дата для фильтра
            filter_type: Тип фильтра ("end" или "start")
            
        Returns:
            Dictionary с фильтром
        """
        if filter_type not in self.FILTER_CONFIGS:
            raise ValueError(f"Неизвестный filter_type: {filter_type}. Используй {list(self.FILTER_CONFIGS.keys())}")
        
        config = self.FILTER_CONFIGS[filter_type]
        filter_date_str = filter_date.strftime("%Y-%m-%dT00:00:00")
        
        return {
            "FilterableEntityName": "BaseObjectAipScheduleWork",
            "Conditions": [
                {
                    "rowId": "ext-1267",
                    "EntityAttributeId": config["entity_attribute"],
                    "Operation": config["operation"],
                    "OperandValue": f'["{filter_date_str}"]',
                    "Not": False,
                    "SubNodes": [
                        {
                            "rowId": config["sub_node_row_id"],
                            "EntityAttributeId": config["sub_node_attribute"],
                            "Operation": 7,
                            "OperandValue": "[]",
                            "Not": False,
                            "SubNodes": []
                        }
                    ]
                }
            ]
        }

    def authorize(self) -> bool:
        """Авторизация через СУДИР OAuth"""
        
        # Инициируем OAuth поток
        start_response = self.session.get(
            "https://mgz.mos.ru/proxy/oauth2/start?rd=%2F",
            allow_redirects=False
        )
        
        if start_response.status_code != 302:
            raise Exception(f"Ошибка инициализации OAuth: {start_response.status_code}")
        
        oauth_ae_url = start_response.headers['Location']
        
        # Переходим на СУДИР OAuth endpoint
        ae_response = self.session.get(oauth_ae_url, allow_redirects=False)
        
        if ae_response.status_code != 303:
            raise Exception(f"Ошибка OAuth AE: {ae_response.status_code}")
        
        # Получаем URL формы логина
        location = ae_response.headers['Location']
        password_url = "https://sudir.mos.ru" + location if location.startswith('/') else location
            
        # Получить форму логина
        form_response = self.session.get(password_url)
        
        if form_response.status_code != 200:
            raise Exception("Не удалось получить форму логина")
        
        # Скрытые поля формы
        soup = BeautifulSoup(form_response.text, 'html.parser')
        form_data = {}
        
        for inp in soup.find_all('input'):
            name = inp.get('name')
            value = inp.get('value', '')
            if name:
                form_data[name] = value
        
        # Добавляем логин/пароль
        form_data['login'] = self.login
        form_data['password'] = self.password
        form_data['isDelayed'] = 'false'
        
        # Отправка формы
        login_response = self.session.post(
            password_url,
            data=form_data,
            allow_redirects=True
        )
        
        # Проверяем, что вернулись на mgz.mos.ru
        if "mgz.mos.ru" not in login_response.url:
            raise Exception(f"Ошибка авторизации: {login_response.url}")
        
        # Извлекаем токен из куки
        oauth_token = self.session.cookies.get("_oauth2_proxy")
        
        if not oauth_token:
            raise Exception("Токен _oauth2_proxy не найден в куках")
        
        self.oauth_token = oauth_token
        
        #  Заходим на главную страницу MGZ чтобы установить куки
        self.session.get("https://mgz.mos.ru/mosks/")
        
        print("\nАвторизация успешна!")
        return True

    def apply_schedule_filter(self, filter_date: datetime = None, filter_type: str = "end", deputy_filter: str = None):
        """
        Применить фильтр по дате для расписания работ
        
        Args:
            filter_date: Дата для фильтра (если None, используется сегодня)
            filter_type: Тип фильтра - "end" (по окончанию) или "start" (по началу)
            deputy_filter: Фильтр по заместителю (например "гиляров" или "ситдиков")
            
        Returns:
            Response object
        """
        if filter_date is None:
            filter_date = datetime.now()
        
        # Строим фильтр используя метод класса
        dynamic_filter = self._build_filter(filter_date, filter_type)
        
        # Строим complexFilter если нужно
        complex_filter = None
        if deputy_filter:
            complex_filter = {
                "left": "Deputy",
                "right": deputy_filter,
                "op": "icontains"
            }
        
        # Payload для POST запроса
        payload = {
            "dynamicFilterJson": json.dumps(dynamic_filter),
            "start": 0,
            "limit": 25,
            "filterTypeValue": True,
            "groupValues": "anode",
            "root": "root",
            "sumColumns": [],
            "groupColumns": [],
            "complexFilter": json.dumps(complex_filter) if complex_filter else None,
            "page": 1,
            "sort": [{"property": "PlanBeginDate", "direction": "ASC"}],
            "id": "root"
        }
        
        # Генерируем timestamp
        timestamp = int(time.time() * 1000)
        
        # Отправляем POST
        response = self.post(
            f"/mosks/action/ScheduleWorkTask/List/?_dc={timestamp}",
            json=payload
        )
        
        if response.status_code != 200:
            raise Exception(f"Ошибка при применении фильтра: {response.status_code}")
        
        description = self.FILTER_CONFIGS[filter_type]["description"]
        deputy_info = f" для {deputy_filter}" if deputy_filter else ""
        print(f"Фильтр применён успешно ({description}{deputy_info})")
        return response


    def download_schedule_excel(self, filter_date: datetime = None, filter_type: str = "end", 
                            deputy_filter: str = None, filename: str = None):
        """
        Скачать файл Excel с расписанием работ
        
        Args:
            filter_date: Дата для фильтра (если None, используется сегодня)
            filter_type: Тип фильтра - "end" (по окончанию) или "start" (по началу)
            deputy_filter: Фильтр по заместителю (например "гиляров" или "ситдиков")
            filename: Имя файла для сохранения (если None, генерируется автоматически)
            
        Returns:
            Путь к скачанному файлу
        """
        if filter_date is None:
            filter_date = datetime.now()
        
        #  Применяем фильтр
        description = self.FILTER_CONFIGS[filter_type]["description"]
        deputy_info = f" для {deputy_filter}" if deputy_filter else ""
        print(f"Применяем фильтр ({description}{deputy_info})...")
        self.apply_schedule_filter(filter_date, filter_type, deputy_filter)
        
        # Собираем фильтр для экспорта
        dynamic_filter = self._build_filter(filter_date, filter_type)
        
        # собрать complexFilter если нужен заместитель
        complex_filter = None
        if deputy_filter:
            complex_filter = {
                "left": "Deputy",
                "right": deputy_filter,
                "op": "icontains"
            }
        
        # Payload для скачивания
        payload = {
            'exportFormat': '14',
            'controllerName': 'ScheduleWorkTask',
            'controllerAction': 'List',
            'filterTypeValue': 'true',
            'anode': 'root',
            'complexFilter': json.dumps(complex_filter) if complex_filter else 'null',
            'dynamicFilterJson': json.dumps(dynamic_filter),
            'columnsInfo': json.dumps(self.EXPORT_COLUMNS).replace('"', "'"),
            'reportGroupColumns': '[]',
            'reportSumColumns': '[]',
            'distinctGroupColumn': '',
            'distinctColumns': '',
            'sort': json.dumps([{"property": "PlanBeginDate", "direction": "ASC"}]),
            'gridName': 'Table1',
            'gridTitle': 'Реестр задач',
            'reportFileName': 'Tasks',
            'showTotalsRow': 'true'
        }
        
        # Генерируем timestamp
        timestamp = int(time.time() * 1000)
        
        # Отправляем POST для скачивания
        print("Скачиваем Excel...")
        response = self.post(
            f"/mosks/action/Print/GetPrintForm/?_dc={timestamp}",
            data=payload
        )
        
        if response.status_code != 200:
            raise Exception(f"Ошибка скачивания: {response.status_code}")
        
        # Определяем имя файла
        if not filename:
            content_disp = response.headers.get('content-disposition', '')
            if 'filename=' in content_disp:
                filename = content_disp.split('filename=')[1].strip('"')
            else:
                deputy_suffix = f"_{deputy_filter}" if deputy_filter else ""
                filename = f"Tasks_{filter_type}{deputy_suffix}_{filter_date.strftime('%Y-%m-%d')}.xlsx"
        
        # Сохраняем файл
        
        os.makedirs(self.download_dir, exist_ok=True)
        with open(f'{self.download_dir}/{filename}', 'wb') as f:
            f.write(response.content)
        
        print(f"Файл скачан: {filename} ({len(response.content)} bytes)")
        return filename

    def get(self, path, **kwargs) -> requests.Response:
        """GET запрос к API"""
        headers = kwargs.pop("headers", {})
        
        headers.setdefault("Accept", "application/json")
        headers.setdefault("X-Requested-With", "XMLHttpRequest")
        
        url = f"{self.base_url}{path}" if path.startswith("/") else path
        response = self.session.get(url, headers=headers, **kwargs)
        
        return response

    def post(self, path, data=None, json=None, **kwargs) -> requests.Response:
        """POST запрос"""
        headers = kwargs.pop("headers", {})
        
        headers.setdefault("Accept", "application/json")
        headers.setdefault("X-Requested-With", "XMLHttpRequest")
        
        url = f"{self.base_url}{path}" if path.startswith("/") else path
        response = self.session.post(
            url, data=data, json=json, headers=headers, **kwargs
        )
        
        return response
    
class Honey_Wagon_Operator:
    '''
    Tough as he was in Afgan
    '''

    def __init__(self):
        self.existing_file_path = Path.cwd() / 'Загрузить_состояние_объектов'
        self.result_file_path = Path.cwd() / 'result'
        self.file_name = '170315 __ Состояние объектов.xlsx'

    def get_file(self):
        cols = ['Код ДС', 'Наименование', 'Зам. руководителя департамента (атрибут)', 'Отрасль', 'Объект ввода', 'Год ввода\n(по плану)', 'Застройщик',
        'Состояние объекта', 'Техническое состояние', 'Тех. состояние. Дата изменения', 'Состояние площадки', 'Руководитель проекта']
        file_path = os.path.join(self.existing_file_path, self.file_name)
        return pd.read_excel(file_path, usecols=cols, header=1)
    
    def transforming_file(self, df):
        week_ago = datetime.now() - timedelta(days=7)
        df['Техническое состояние (всего символов)'] = df['Техническое состояние'].str.len()
        mask_and = (
            (df['Зам. руководителя департамента (атрибут)'].isin(('Ситдиков Н.Р.', 'Гиляров В.В.'))) &
            (df['Объект ввода'] == 'да') &
            (df['Год ввода\n(по плану)'] > 2025))
        df_masked_1 = df[mask_and].copy()
        mask_or = ((
            (df_masked_1['Тех. состояние. Дата изменения'].isna()) | 
            (pd.to_datetime(df_masked_1['Тех. состояние. Дата изменения'], errors='coerce') < week_ago)
        ) |
        ((df_masked_1['Состояние площадки'].isna()) & (df_masked_1['Состояние объекта'].isin(('В строительстве', 'Строительство завершено', 'Строительство приостановлено')))) |
        (df_masked_1['Руководитель проекта'].isna()) |
        (
            (df_masked_1['Техническое состояние'].isna()) |
            (df_masked_1['Техническое состояние'] == '') | 
            (df_masked_1['Техническое состояние'].str.contains('%', na=False)) |
            (df_masked_1['Техническое состояние (всего символов)'] > 400)
        ))
        df_masked_2 = df_masked_1[mask_or]
        df_masked_2['Наличие %'] = df_masked_2['Техническое состояние'].str.contains('%', na=False)
        df_masked_2['Больше 400 символов'] = df_masked_2['Техническое состояние (всего символов)'] > 400
        return df_masked_2[['Код ДС', 'Наименование', 'Зам. руководителя департамента (атрибут)', 'Отрасль', 'Объект ввода', 'Год ввода\n(по плану)', 'Застройщик',
        'Состояние объекта', 'Техническое состояние', 'Техническое состояние (всего символов)', 'Наличие %', 'Больше 400 символов', 'Тех. состояние. Дата изменения', 'Состояние площадки', 'Руководитель проекта']]
    
    def save_file(self, df):
        file_path = os.path.join(self.result_file_path, self.file_name)
        df.to_excel(file_path, index=False)
        return
    
    def full_pipe(self):
        try:
            df = self.get_file()
            df = self.transforming_file(df)
            self.save_file(df)
        except Exception as e:
            print(f'Ошибка {e}')

def transform_and_save_dfs(dfs_list, client, output_file_name, columns_to_drop):
        dfs_new = []
        for i in dfs_list:
            dfs_new.append(i.drop(columns=['Главная работа', 'Статус', 'Согласовано исполнителем', 'Согласовавший оператор', 'Дата согласования', 'Префектура',
                    'Состояние текущего предлагаемого решения (начало)', 'Кол-во переносов начала задачи', 'Кол-во предложенных решений (начало)',
                    'нет заголовка', 'нет заголовка.1', 'Состояние текущего предлагаемого решения (окончание)', 'Кол-во переносов окончания задачи', 'Кол-во предложенных решений (окончание)',
                    'План. дней', 'Факт. дней', 'Смещение', 'Процент', 'Утв. начало', 'Утв. окончание', 'Утв. дней', 'Смещение начала по утв.',
                        'Смещение окончания по утв.', 'Дата утверждения']))
        if len(dfs_list) > 1:
            transformed_file = pd.concat(dfs_new, ignore_index=True)
        else:
            transformed_file = dfs_new[0]
        mask = transformed_file['Заместитель'].isin(['Гиляров В.В.', 'Ситдиков Н.Р.'])
        transformed_file = transformed_file[mask]
        os.makedirs(client.result_dir, exist_ok=True)
        transformed_file.drop(columns=columns_to_drop).to_excel(f'{client.result_dir}/{output_file_name}', index=False)
        return 0
    
if __name__ == '__main__':

    date_start = input('Введите дату для даты начала (ДД.ММ.ГГГГ) или Enter для сегодня: ').strip()
    try:
        date_start = datetime.strptime(date_start, "%d.%m.%Y") if date_start else date.today()
    except ValueError:
        print("Неверный формат! Используется сегодняшняя дата.")
        date_start = date.today()

    date_end = input('Введите дату для даты окончания (ДД.ММ.ГГГГ) или Enter для сегодня:').strip()
    try:
        date_end = datetime.strptime(date_end, "%d.%m.%Y") if date_end else date.today()
    except ValueError:
        print("Неверный формат! Используется сегодняшняя дата.")
        date_end = date.today()

    max_attempts = 3

    for attempt in range(1, max_attempts + 1):
        sudir_login = input("Введите логин СУДИР:\n")
        sudir_password = getpass.getpass("Введите пароль СУДИР:\n")
        client = MgzClient(sudir_login, sudir_password)
        
        try:
            client.authorize()
            break
        except Exception as e:
            print(f'Ошибка авторизации: {e}')
            if attempt < max_attempts:
                print(f'Попытка {attempt}/{max_attempts}. Попробуйте снова.\n')
            else:
                print('Превышено количество попыток!')
                raise
      
    client.download_schedule_excel(date_start, filter_type='start', deputy_filter='гиляров', filename='начало_гиляров.xlsx')
    client.download_schedule_excel(date_start, filter_type='start', deputy_filter='ситдиков', filename='начало_ситдиков.xlsx')
    client.download_schedule_excel(date_end, filter_type='end', filename='окончание.xlsx')
    print('Сохранено в папке download')
    dfs_raw = [pd.read_excel(f, header=2) for f in Path(client.download_dir).glob('начало*.xlsx')]
    transform_and_save_dfs(dfs_list=dfs_raw, client=client, output_file_name='начало.xlsx', columns_to_drop=['План. окончание', 'Факт. окончание'])
    dfs_raw = [pd.read_excel(f, header=2) for f in Path(client.download_dir).glob('окончание.xlsx')]
    transform_and_save_dfs(dfs_list=dfs_raw, client=client, output_file_name='окончание.xlsx', columns_to_drop=['План. начало', 'Факт. начало'])
    print('Обработка состояния объектов')
    new_file = Honey_Wagon_Operator()
    new_file.full_pipe()
    input('Готово, файлы в папке result')
    