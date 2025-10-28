import sys
import asyncio
import json
import time
from datetime import datetime
import pandas as pd
import aiofiles
import aiohttp
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, 
    QLabel, QLineEdit, QPushButton, QTextEdit, QFileDialog, 
    QMessageBox, QProgressBar, QGroupBox, QCheckBox, QRadioButton, QTextBrowser
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
import os


# Все доступные поля и фильтры, как на сайте VK
FIELDS_DESCRIPTION = {
    "bdate": "Дата рождения пользователя",
    "can_post": "Может ли пользователь писать на стену",
    "can_see_all_posts": "Может видеть все посты",
    "can_see_audio": "Может видеть аудиозаписи",
    "can_write_private_message": "Может писать личные сообщения",
    "city": "Город пользователя",
    "common_count": "Количество общих друзей",
    "connections": "Информация из сторонних сервисов (Twitter, Instagram и др.)",
    "contacts": "Контактная информация",
    "country": "Страна пользователя",
    "domain": "Короткий адрес страницы пользователя",
    "education": "Образование пользователя",
    "has_mobile": "Есть ли мобильный телефон",
    "last_seen": "Время последнего посещения VK",
    "lists": "Пользовательские списки",
    "online": "В сети ли сейчас",
    "online_mobile": "Через мобильное приложение",
    "photo_100": "Фото пользователя 100x100 px",
    "photo_200": "Фото пользователя 200x200 px",
    "photo_200_orig": "Оригинальное фото 200x200 px",
    "photo_400_orig": "Оригинальное фото 400x400 px",
    "photo_50": "Фото пользователя 50x50 px",
    "photo_max": "Максимальное фото пользователя",
    "photo_max_orig": "Оригинальное максимальное фото",
    "relation": "Семейное положение",
    "relatives": "Родственники",
    "schools": "Школы пользователя",
    "sex": "Пол пользователя",
    "site": "Персональный сайт",
    "status": "Статус пользователя",
    "universities": "Вузы"
}

FILTERS_DESCRIPTION = {
    "friends": "Только друзья",
    "unsure": "Выбрали «Возможно пойду» (мероприятия)",
    "managers": "Руководители сообщества (требуется токен администратора)",
    "donut": "VK Donut подписчики",
    "unsure_friends": "Друзья с выбором «Возможно пойду»",
    "invites": "Приглашённые пользователи (мероприятие)"
}


def fields_help():
    help_text = "Описание всех fields (дополнительные данные):\n"
    help_text += "=" * 50 + "\n"
    for k, v in FIELDS_DESCRIPTION.items():
        help_text += f"{k}: {v}\n"
    return help_text


def filters_help():
    help_text = "Описание всех filter (фильтры по составу):\n"
    help_text += "=" * 50 + "\n"
    for k, v in FILTERS_DESCRIPTION.items():
        help_text += f"{k}: {v}\n"
    help_text += "\nВАЖНО: Для получения данных по параметру filter (например, \"managers\", \"donut\", \"friends\")\n"
    help_text += "требуется админский access_token — токен владельца или администратора сообщества.\n"
    help_text += "Иначе VK API выдаст ошибку доступа и не покажет нужный список.\n"
    return help_text


def parameters_help():
    help_text = "Параметры получения участников группы:\n"
    help_text += "=" * 50 + "\n"
    help_text += "count: Сколько участников получить (1–1000). По умолчанию 1000.\n"
    help_text += "offset: Смещение, необходимое для выборки определённого подмножества участников.\n"
    help_text += "        Например, если offset=1000 и count=1000, то будут получены участники с 1001 по 2000.\n"
    help_text += "        По умолчанию 0.\n"
    help_text += "sort: Сортировка результатов:\n"
    help_text += "      \"id_asc\" — по возрастанию ID (по умолчанию)\n"
    help_text += "      \"id_desc\" — по убыванию ID\n"
    help_text += "      \"time_asc\"/\"time_desc\" — по времени вступления \n"
    help_text += "      (требуется токен модератора)\n"
    help_text += "fields: Дополнительные поля профилей участников через запятую.\n"
    help_text += "        Например: \"city,sex,bdate\". По умолчанию пусто.\n"
    help_text += "filter: Фильтр по составу участников:\n"
    help_text += "        \"friends\" — только друзья\n"
    help_text += "        \"managers\" — руководители сообщества\n"
    help_text += "        \"donut\" — VK Donut подписчики\n"
    help_text += "        и др. (см. справку по фильтрам)\n"
    help_text += "\nПример использования смещения:\n"
    help_text += "Запрос 1: offset=0, count=1000 → участники 1-1000\n"
    help_text += "Запрос 2: offset=1000, count=1000 → участники 1001-2000\n"
    help_text += "Запрос 3: offset=2000, count=1000 → участники 2001-3000\n"
    return help_text


class VKGroupMembers:
    def __init__(self, token, group_id):
        self.token = token
        self.group_id = group_id
        self.members_data = []

    async def get_group_members(self, count=1000, offset=0, sort=None, fields=None, filter_param=None):
        params = {
            'access_token': self.token,
            'group_id': self.group_id,
            'v': '5.131',
            'count': count,
            'offset': offset
        }
        if sort:
            params['sort'] = sort
        if fields:
            params['fields'] = fields
        if filter_param:
            params['filter'] = filter_param
            
        url = "https://api.vk.com/method/groups.getMembers"
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as r:
                result = await r.json()
                if "response" in result:
                    self.members_data = result["response"]["items"]
                    return result
                else:
                    raise Exception(f"Ошибка VK API: {result}")

    def export_json(self, filename="group_members.json"):
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.members_data, f, ensure_ascii=False, indent=2)

    def export_csv(self, filename="group_members.csv"):
        df = pd.json_normalize(self.members_data)
        df.to_csv(filename, index=False, encoding="utf-8-sig")

    def print_summary(self):
        return {
            'total': len(self.members_data)
        }


class VKParser:
    def __init__(self, domain, token, owner_id, delay=0.35, count=10, time_period=60 * 60 * 24 * 30, proxy=None, filter_keywords=False):
        # Configuration
        self.TOKEN = token
        self.DOMAIN = domain  # Community address
        self.COUNT = count  # Number of posts per request
        self.delay = delay  # Delay in seconds
        self.lastRequestTime = 0
        self.time_period = time_period
        self.proxy = proxy
        self.owner_id = owner_id
        self.parsed_data = []  # Store parsed data
        self.filter_keywords = filter_keywords  # Enable keyword filtering
        self.keywords = []  # Keywords for filtering

    # Load keywords for filtering
    async def load_keywords(self):
        try:
            async with aiofiles.open('data/words.txt', mode='r', encoding='utf-8') as file:
                self.keywords = [row.strip().lower() async for row in file]
        except Exception as e:
            print(f'[Ошибка] Не удалось загрузить ключевые слова: {e}')
            self.keywords = []

    # Check if text matches keywords
    async def check_keywords(self, text):
        if not self.keywords or not self.filter_keywords:
            return True  # No filtering if keywords not loaded or filtering disabled
        
        # Simple keyword matching
        text_lower = text.lower()
        for keyword in self.keywords:
            if keyword in text_lower:
                return True
        return False

    # API requests
    async def requests_func(self, method, url_params):
        url = f'https://api.vk.com/method/{method}?v=5.131&access_token={self.TOKEN}&{url_params}'
        while True:
            await asyncio.sleep(0.01)
            if self.lastRequestTime + self.delay < time.time():
                self.lastRequestTime = time.time()

                async with aiohttp.ClientSession() as session:
                    async with session.post(url, proxy=self.proxy) as response:
                        data = await response.text()
                        return data

    # Main function to parse data
    async def parse_data(self, progress_callback=None):
        print(f'[{self.DOMAIN}] Начало парсинга...')
        
        # Load keywords if filtering is enabled
        if self.filter_keywords:
            await self.load_keywords()
            if self.keywords:
                print(f"Загружено {len(self.keywords)} ключевых слов для фильтрации")
            else:
                print("Фильтрация включена, но ключевые слова не найдены")
        
        url = "wall.get", f"domain={self.DOMAIN}&count={self.COUNT}"

        req_posts = await self.requests_func(*url)
        try:
            posts = json.loads(req_posts)['response']['items']
        except Exception as e:
            print(f'[Ошибка] {e}')
            return

        total_posts = len(posts)
        for i, post in enumerate(posts):
            # Parse post
            await self.parse_post(post)

            # Parse comments if there are any
            if int(post['comments']['count']) > 0:
                await self.parse_comments(post)
            
            # Report progress
            if progress_callback:
                progress_callback(int((i + 1) / total_posts * 100))

    # Parse posts
    async def parse_post(self, post):
        await asyncio.sleep(0.01)
        
        # Check keywords if filtering is enabled
        if self.filter_keywords:
            if not await self.check_keywords(post['text']):
                return  # Skip post if it doesn't match keywords
        
        # Collect photo and video information
        photo = {}
        video = {}
        if 'attachments' in post.keys():
            for i in range(len(post['attachments'])):
                if 'video' in post['attachments'][i].keys():
                    video[len(video)] = post['attachments'][i]['video']['image'][-1]['url']
                elif 'photo' in post['attachments'][i].keys():
                    photo[len(photo)] = post['attachments'][i]['photo']['sizes'][-1]['url']

        # Collect general post information
        post_data = {
            'type': 'post',
            'date': datetime.utcfromtimestamp(int(post['date'])).strftime('%Y-%m-%d %H:%M:%S'),
            'user_id': post['owner_id'],
            'text': str(post['text'].replace("'", "").replace("\n\n", "\n")),
            'photo_count': len(photo),
            'video_count': len(video),
            'comments_count': int(post['comments']['count']),
            'likes_count': int(post['likes']['count']) if 'likes' in post else 0,
            'reposts_count': int(post['reposts']['count']) if 'reposts' in post else 0,
            'views_count': int(post['views']['count']) if 'views' in post else 0,
            'link': f'https://vk.com/{self.DOMAIN}?w=wall-{self.owner_id}_{post["id"]}',
            'post_id': post['id']
        }

        self.parsed_data.append(post_data)

    # Parse comments
    async def parse_comments(self, post):
        await asyncio.sleep(0.01)
        
        # Note: owner_id should be negative for communities
        for offset in range(0, int(post['comments']['count']) + 100, 100):
            url = ["wall.getComments", f"owner_id={self.owner_id}&post_id={post['id']}&count={100}&offset={offset}&extended=1"]

            comments_full = json.loads(await self.requests_func(*url))

            if 'response' in comments_full.keys():
                comments = comments_full['response']['items']
                profiles = comments_full['response']['profiles']

                for comment in comments:
                    # Check keywords if filtering is enabled
                    if self.filter_keywords:
                        if not await self.check_keywords(comment['text']):
                            continue  # Skip comment if it doesn't match keywords
                    
                    # Get user info
                    first_name = ""
                    last_name = ""
                    
                    for profile in profiles:
                        if comment['from_id'] == profile['id']:
                            first_name = profile['first_name']
                            last_name = profile['last_name']
                            break

                    # Collect photo and video information
                    photo = {}
                    video = {}
                    if 'attachments' in comment.keys():
                        for k in range(len(comment['attachments'])):
                            if 'video' in comment['attachments'][k].keys():
                                video[len(video)] = comment['attachments'][k]['video']['image'][-1]['url']
                            elif 'photo' in comment['attachments'][k].keys():
                                photo[len(photo)] = comment['attachments'][k]['photo']['sizes'][-1]['url']

                    # Collect general comment information
                    date = datetime.utcfromtimestamp(int(comment['date'])).strftime('%Y-%m-%d %H:%M:%S')
                    comment_data = {
                        'type': 'comment',
                        'date': date,
                        'user_id': str(comment['from_id']),
                        'first_name': first_name,
                        'last_name': last_name,
                        'text': str(comment['text']),
                        'photo_count': len(photo),
                        'video_count': len(video),
                        'likes_count': int(comment['likes']['count']) if 'likes' in comment else 0,
                        'post_link': f'https://vk.com/{self.DOMAIN}?w=wall-{self.owner_id}_{post["id"]}',
                        'post_id': post['id'],
                        'comment_id': comment['id']
                    }

                    self.parsed_data.append(comment_data)

                    # Parse comment threads/replies
                    if 'thread' in comment and comment['thread']['count'] > 0:
                        await self.parse_comment_thread(post, comment, profiles)

    # Parse comment threads
    async def parse_comment_thread(self, post, comment, profiles):
        # Note: owner_id should be negative for communities
        for offset in range(0, comment['thread']['count'] + 100, 100):
            url = ["wall.getComments", f"owner_id={self.owner_id}&post_id={post['id']}&comment_id={comment['id']}&count={100}&offset={offset}&extended=1"]

            comments_thread_full = json.loads(await self.requests_func(*url))

            if 'response' in comments_thread_full.keys():
                comments_thread = comments_thread_full['response']['items']
                profiles_thread = comments_thread_full['response']['profiles']

                for comment_thread in comments_thread:
                    # Check keywords if filtering is enabled
                    if self.filter_keywords:
                        if not await self.check_keywords(comment_thread['text']):
                            continue  # Skip reply if it doesn't match keywords
                    
                    # Get user info
                    first_name = ""
                    last_name = ""
                    
                    for profile in profiles_thread:
                        if comment_thread['from_id'] == profile['id']:
                            first_name = profile['first_name']
                            last_name = profile['last_name']
                            break

                    # Collect photo and video information
                    photo = {}
                    video = {}
                    if 'attachments' in comment_thread.keys():
                        for k in range(len(comment_thread['attachments'])):
                            if 'video' in comment_thread['attachments'][k].keys():
                                video[len(video)] = comment_thread['attachments'][k]['video']['image'][-1]['url']
                            elif 'photo' in comment_thread['attachments'][k].keys():
                                photo[len(photo)] = comment_thread['attachments'][k]['photo']['sizes'][-1]['url']

                    # Collect general comment information
                    date = datetime.utcfromtimestamp(int(comment_thread['date'])).strftime('%Y-%m-%d %H:%M:%S')
                    comment_data = {
                        'type': 'reply',
                        'date': date,
                        'user_id': str(comment_thread['from_id']),
                        'first_name': first_name,
                        'last_name': last_name,
                        'text': str(comment_thread['text']),
                        'photo_count': len(photo),
                        'video_count': len(video),
                        'likes_count': int(comment_thread['likes']['count']) if 'likes' in comment_thread else 0,
                        'post_link': f'https://vk.com/{self.DOMAIN}?w=wall-{self.owner_id}_{post["id"]}',
                        'post_id': post['id'],
                        'parent_comment_id': comment['id'],
                        'comment_id': comment_thread['id']
                    }

                    self.parsed_data.append(comment_data)

    # Export data to JSON
    def export_to_json(self, filename='vk_data.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.parsed_data, f, ensure_ascii=False, indent=4)
        print(f"Данные экспортированы в {filename}")

    # Export data to Excel
    def export_to_excel(self, filename='vk_data.xlsx'):
        df = pd.DataFrame(self.parsed_data)
        df.to_excel(filename, index=False)
        print(f"Данные экспортированы в {filename}")

    # Export data to CSV
    def export_to_csv(self, filename='vk_data.csv'):
        df = pd.DataFrame(self.parsed_data)
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Данные экспортированы в {filename}")

    # Print summary in Russian
    def print_summary(self):
        posts_count = sum(1 for item in self.parsed_data if item['type'] == 'post')
        comments_count = sum(1 for item in self.parsed_data if item['type'] == 'comment')
        replies_count = sum(1 for item in self.parsed_data if item['type'] == 'reply')
        
        return {
            'total': len(self.parsed_data),
            'posts': posts_count,
            'comments': comments_count,
            'replies': replies_count
        }


class MembersWorker(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, token, group_id, count, offset, sort, fields, filter_param):
        super().__init__()
        self.token = token
        self.group_id = group_id
        self.count = count
        self.offset = offset
        self.sort = sort
        self.fields = fields
        self.filter_param = filter_param

    def run(self):
        try:
            # Create asyncio event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Create members instance
            members = VKGroupMembers(self.token, self.group_id)
            
            # Run fetching members
            result = loop.run_until_complete(members.get_group_members(
                count=self.count,
                offset=self.offset,
                sort=self.sort,
                fields=self.fields,
                filter_param=self.filter_param
            ))
            
            # Emit results
            self.finished.emit((members, result))
        except Exception as e:
            self.error.emit(str(e))


class ParserWorker(QThread):
    progress = pyqtSignal(int)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(self, domain, token, owner_id, count):
        super().__init__()
        self.domain = domain
        self.token = token
        self.owner_id = owner_id
        self.count = count

    def run(self):
        try:
            # Create asyncio event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            
            # Create parser instance
            parser = VKParser(self.domain, self.token, self.owner_id, count=self.count, filter_keywords=False)
            
            # Run parsing
            loop.run_until_complete(parser.parse_data(self.progress.emit))
            
            # Emit results
            self.finished.emit(parser)
        except Exception as e:
            self.error.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("VK Group Parser")
        self.setGeometry(100, 100, 700, 650)
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Create mode selection
        mode_group = QGroupBox("Режим работы")
        mode_layout = QHBoxLayout()
        
        self.parse_mode_radio = QRadioButton("Парсинг постов/комментариев")
        self.members_mode_radio = QRadioButton("Получение участников группы")
        self.parse_mode_radio.setChecked(True)
        
        mode_layout.addWidget(self.parse_mode_radio)
        mode_layout.addWidget(self.members_mode_radio)
        mode_group.setLayout(mode_layout)
        main_layout.addWidget(mode_group)
        
        # Create input group
        self.input_group = QGroupBox("Параметры парсинга")
        self.input_layout = QVBoxLayout()
        
        # Token instructions
        token_instructions = QLabel("Для работы программы необходим токен VK:\n" +
                                   "1. Создайте Standalone-приложение: https://vk.com/apps?act=manage\n" +
                                   "2. Получите токен для работы\n" +
                                   "3. Если у группы нет числового ID, получите его тут: https://regvk.com/id")
        token_instructions.setWordWrap(True)
        token_instructions.setStyleSheet("color: gray; font-size: 9pt;")
        self.input_layout.addWidget(token_instructions)
        
        # Token input
        token_layout = QHBoxLayout()
        token_layout.addWidget(QLabel("Токен VK:"))
        self.token_input = QLineEdit()
        self.token_input.setEchoMode(QLineEdit.EchoMode.Password)
        token_layout.addWidget(self.token_input)
        self.input_layout.addLayout(token_layout)
        
        # Owner ID input (for parsing)
        self.owner_layout = QHBoxLayout()
        self.owner_layout.addWidget(QLabel("Owner ID:"))
        self.owner_input = QLineEdit()
        self.owner_layout.addWidget(self.owner_input)
        self.input_layout.addLayout(self.owner_layout)
        
        # Domain input (for parsing)
        self.domain_layout = QHBoxLayout()
        self.domain_layout.addWidget(QLabel("Имя группы:"))
        self.domain_input = QLineEdit()
        self.domain_layout.addWidget(self.domain_input)
        self.input_layout.addLayout(self.domain_layout)
        
        # Count input (for parsing)
        self.count_layout = QHBoxLayout()
        self.count_layout.addWidget(QLabel("Количество постов:"))
        self.count_input = QLineEdit("10")
        self.count_layout.addWidget(self.count_input)
        self.input_layout.addLayout(self.count_layout)
        
        self.input_group.setLayout(self.input_layout)
        main_layout.addWidget(self.input_group)
        
        # Members parameters group
        self.members_group = QGroupBox("Параметры получения участников")
        self.members_layout = QVBoxLayout()
        
        # Members instructions
        members_instructions = QLabel("Для получения участников группы:\n" +
                                     "1. Введите ID группы (число)\n" +
                                     "2. Укажите количество участников (1-1000)\n" +
                                     "3. При необходимости укажите дополнительные параметры")
        members_instructions.setWordWrap(True)
        members_instructions.setStyleSheet("color: gray; font-size: 9pt;")
        self.members_layout.addWidget(members_instructions)
        
        # Group ID input (for members)
        group_id_layout = QHBoxLayout()
        group_id_layout.addWidget(QLabel("ID группы:"))
        self.group_id_input = QLineEdit()
        group_id_layout.addWidget(self.group_id_input)
        self.members_layout.addLayout(group_id_layout)
        
        # Members count input
        members_count_layout = QHBoxLayout()
        members_count_layout.addWidget(QLabel("Количество участников (1-1000):"))
        self.members_count_input = QLineEdit("1000")
        members_count_layout.addWidget(self.members_count_input)
        self.members_layout.addLayout(members_count_layout)
        
        # Offset input
        offset_layout = QHBoxLayout()
        offset_layout.addWidget(QLabel("Смещение (offset):"))
        self.offset_input = QLineEdit("0")
        offset_layout.addWidget(self.offset_input)
        self.members_layout.addLayout(offset_layout)
        
        # Sort input
        sort_layout = QHBoxLayout()
        sort_layout.addWidget(QLabel("Сортировка:"))
        self.sort_input = QLineEdit("id_asc")
        sort_layout.addWidget(self.sort_input)
        self.members_layout.addLayout(sort_layout)
        
        # Fields input
        fields_layout = QHBoxLayout()
        fields_layout.addWidget(QLabel("Поля (через запятую):"))
        self.fields_input = QLineEdit()
        fields_layout.addWidget(self.fields_input)
        self.members_layout.addLayout(fields_layout)
        
        # Filter input
        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Фильтр:"))
        self.filter_input = QLineEdit()
        filter_layout.addWidget(self.filter_input)
        self.members_layout.addLayout(filter_layout)
        
        # Help buttons
        help_layout = QHBoxLayout()
        self.parameters_help_button = QPushButton("Справка по параметрам")
        self.parameters_help_button.clicked.connect(self.show_parameters_help)
        self.fields_help_button = QPushButton("Справка по полям")
        self.fields_help_button.clicked.connect(self.show_fields_help)
        self.filters_help_button = QPushButton("Справка по фильтрам")
        self.filters_help_button.clicked.connect(self.show_filters_help)
        help_layout.addWidget(self.parameters_help_button)
        help_layout.addWidget(self.fields_help_button)
        help_layout.addWidget(self.filters_help_button)
        self.members_layout.addLayout(help_layout)
        
        self.members_group.setLayout(self.members_layout)
        self.members_group.setVisible(False)
        main_layout.addWidget(self.members_group)
        
        # Create buttons
        buttons_layout = QHBoxLayout()
        
        self.start_button = QPushButton("Начать парсинг")
        self.start_button.clicked.connect(self.start_parsing)
        buttons_layout.addWidget(self.start_button)
        
        self.export_button = QPushButton("Экспорт данных")
        self.export_button.clicked.connect(self.export_data)
        self.export_button.setEnabled(False)
        buttons_layout.addWidget(self.export_button)
        
        main_layout.addLayout(buttons_layout)
        
        # Create progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setValue(0)
        main_layout.addWidget(self.progress_bar)
        
        # Create log area
        self.log_area = QTextEdit()
        self.log_area.setReadOnly(True)
        main_layout.addWidget(self.log_area)
        
        # Create contact button
        contact_layout = QHBoxLayout()
        contact_layout.addStretch()
        self.contact_button = QPushButton("Связаться")
        self.contact_button.clicked.connect(self.open_contact)
        contact_layout.addWidget(self.contact_button)
        main_layout.addLayout(contact_layout)
        
        # Parser result storage
        self.parser_result = None
        
        # Connect mode radio buttons
        self.parse_mode_radio.toggled.connect(self.on_mode_changed)
        self.members_mode_radio.toggled.connect(self.on_mode_changed)
        
        # Set example values (without actual token for security)
        self.domain_input.setText("ddx_fitness")
        self.owner_input.setText("-164992662")
        self.token_input.setPlaceholderText("Введите ваш токен VK")
        self.group_id_input.setText("191570013")
        self.group_id_input.setToolTip("ID группы VK (число, например: 191570013)")
        self.members_count_input.setToolTip("Количество участников за один запрос (1-1000)")
        self.offset_input.setToolTip("Смещение для получения следующих участников")
        self.sort_input.setToolTip("Сортировка: id_asc, id_desc, time_asc, time_desc")
        self.fields_input.setToolTip("Доп. поля через запятую: city,sex,bdate и др.")
        self.filter_input.setToolTip("Фильтр: friends, managers, donut и др. (требуется админ токен)")
        
    def on_mode_changed(self):
        if self.parse_mode_radio.isChecked():
            self.input_group.setVisible(True)
            self.members_group.setVisible(False)
        else:
            self.input_group.setVisible(False)
            self.members_group.setVisible(True)
        
    def show_parameters_help(self):
        help_text = parameters_help()
        QMessageBox.information(self, "Справка по параметрам", help_text)
        
    def show_fields_help(self):
        help_text = fields_help()
        QMessageBox.information(self, "Справка по полям", help_text)
        
    def show_filters_help(self):
        help_text = filters_help()
        QMessageBox.information(self, "Справка по фильтрам", help_text)
        
    def log_message(self, message):
        self.log_area.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        
    def start_parsing(self):
        if self.parse_mode_radio.isChecked():
            self.start_post_parsing()
        else:
            self.start_members_parsing()
            
    def start_post_parsing(self):
        # Get input values
        domain = self.domain_input.text().strip()
        owner_id = self.owner_input.text().strip()
        token = self.token_input.text().strip()
        count = self.count_input.text().strip()
        
        # Validate inputs
        if not domain:
            QMessageBox.warning(self, "Ошибка", "Введите имя группы")
            return
            
        if not owner_id:
            QMessageBox.warning(self, "Ошибка", "Введите Owner ID")
            return
            
        if not token:
            QMessageBox.warning(self, "Ошибка", "Введите токен")
            return
            
        try:
            count = int(count)
            if count <= 0:
                raise ValueError("Count must be positive")
        except ValueError:
            QMessageBox.warning(self, "Ошибка", "Количество постов должно быть положительным числом")
            return
            
        # Disable start button and reset progress
        self.start_button.setEnabled(False)
        self.export_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.log_area.clear()
        
        # Create and start worker thread
        self.worker = ParserWorker(domain, token, owner_id, count)
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.parsing_finished)
        self.worker.error.connect(self.parsing_error)
        self.worker.start()
        
        self.log_message("Начало парсинга постов/комментариев...")
        
    def start_members_parsing(self):
        # Get input values
        token = self.token_input.text().strip()
        group_id = self.group_id_input.text().strip()
        count = self.members_count_input.text().strip()
        offset = self.offset_input.text().strip()
        sort = self.sort_input.text().strip() or None
        fields = self.fields_input.text().strip() or None
        filter_param = self.filter_input.text().strip() or None
        
        # Validate inputs
        if not group_id:
            QMessageBox.warning(self, "Ошибка", "Введите ID группы")
            return
            
        if not token:
            QMessageBox.warning(self, "Ошибка", "Введите токен")
            return
            
        try:
            count = int(count)
            if count <= 0 or count > 1000:
                raise ValueError("Count must be between 1 and 1000")
        except ValueError:
            QMessageBox.warning(self, "Ошибка", "Количество участников должно быть числом от 1 до 1000")
            return
            
        try:
            offset = int(offset)
        except ValueError:
            QMessageBox.warning(self, "Ошибка", "Смещение должно быть числом")
            return
            
        # Disable start button and reset progress
        self.start_button.setEnabled(False)
        self.export_button.setEnabled(False)
        self.progress_bar.setValue(0)
        self.log_area.clear()
        
        # Create and start worker thread
        self.worker = MembersWorker(token, group_id, count, offset, sort, fields, filter_param)
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.members_finished)
        self.worker.error.connect(self.parsing_error)
        self.worker.start()
        
        self.log_message("Начало получения участников группы...")
        
    def update_progress(self, value):
        self.progress_bar.setValue(value)
        
    def parsing_finished(self, parser):
        self.parser_result = parser
        self.start_button.setEnabled(True)
        self.export_button.setEnabled(True)
        self.progress_bar.setValue(100)
        
        # Show summary
        summary = parser.print_summary()
        self.log_message(f"Парсинг завершен!")
        self.log_message(f"Всего записей: {summary['total']}")
        self.log_message(f"Постов: {summary['posts']}")
        self.log_message(f"Комментариев: {summary['comments']}")
        self.log_message(f"Ответов: {summary['replies']}")
        
    def members_finished(self, result):
        members, api_result = result
        self.parser_result = members
        self.start_button.setEnabled(True)
        self.export_button.setEnabled(True)
        self.progress_bar.setValue(100)
        
        # Show summary
        summary = members.print_summary()
        self.log_message(f"Получение участников завершено!")
        self.log_message(f"Всего участников: {summary['total']}")
        if 'response' in api_result and 'count' in api_result['response']:
            self.log_message(f"Общее количество участников в группе: {api_result['response']['count']}")
        
    def parsing_error(self, error_message):
        self.start_button.setEnabled(True)
        self.export_button.setEnabled(False)
        self.progress_bar.setValue(0)
        QMessageBox.critical(self, "Ошибка", f"Произошла ошибка:\n{error_message}")
        self.log_message(f"Ошибка: {error_message}")
        
    def export_data(self):
        if not self.parser_result:
            QMessageBox.warning(self, "Ошибка", "Нет данных для экспорта")
            return
            
        # Ask user for export directory
        directory = QFileDialog.getExistingDirectory(self, "Выберите папку для экспорта")
        if not directory:
            return
            
        try:
            if isinstance(self.parser_result, VKParser):
                # Export parsing results
                domain = self.domain_input.text().strip()
                filename_base = f"{domain}_data"
                
                # Export to JSON
                json_path = os.path.join(directory, f"{filename_base}.json")
                self.parser_result.export_to_json(json_path)
                
                # Export to CSV
                csv_path = os.path.join(directory, f"{filename_base}.csv")
                self.parser_result.export_to_csv(csv_path)
                
                self.log_message(f"Данные экспортированы в:")
                self.log_message(f"  - {json_path}")
                self.log_message(f"  - {csv_path}")
            else:
                # Export members results
                group_id = self.group_id_input.text().strip() or "group"
                filename_base = f"{group_id}_members"
                
                # Export to JSON
                json_path = os.path.join(directory, f"{filename_base}.json")
                self.parser_result.export_json(json_path)
                
                # Export to CSV
                csv_path = os.path.join(directory, f"{filename_base}.csv")
                self.parser_result.export_csv(csv_path)
                
                self.log_message(f"Данные экспортированы в:")
                self.log_message(f"  - {json_path}")
                self.log_message(f"  - {csv_path}")
            
            QMessageBox.information(self, "Успех", f"Данные успешно экспортированы в папку:\n{directory}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Произошла ошибка при экспорте:\n{str(e)}")
            self.log_message(f"Ошибка экспорта: {str(e)}")
            
    def open_contact(self):
        import webbrowser
        webbrowser.open("https://t.me/Userspoi")


def main():
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
