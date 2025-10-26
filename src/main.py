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
    QMessageBox, QProgressBar, QGroupBox, QCheckBox
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
import os


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
        self.setGeometry(100, 100, 600, 500)
        
        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        # Create input group
        input_group = QGroupBox("Параметры парсинга")
        input_layout = QVBoxLayout()
        
        # Token input
        token_layout = QHBoxLayout()
        token_layout.addWidget(QLabel("Токен VK:"))
        self.token_input = QLineEdit()
        self.token_input.setEchoMode(QLineEdit.EchoMode.Password)
        token_layout.addWidget(self.token_input)
        input_layout.addLayout(token_layout)
        
        # Owner ID input
        owner_layout = QHBoxLayout()
        owner_layout.addWidget(QLabel("Owner ID:"))
        self.owner_input = QLineEdit()
        owner_layout.addWidget(self.owner_input)
        input_layout.addLayout(owner_layout)
        
        # Domain input
        domain_layout = QHBoxLayout()
        domain_layout.addWidget(QLabel("Имя группы:"))
        self.domain_input = QLineEdit()
        domain_layout.addWidget(self.domain_input)
        input_layout.addLayout(domain_layout)
        
        # Count input
        count_layout = QHBoxLayout()
        count_layout.addWidget(QLabel("Количество постов:"))
        self.count_input = QLineEdit("10")
        count_layout.addWidget(self.count_input)
        input_layout.addLayout(count_layout)
        
        input_group.setLayout(input_layout)
        main_layout.addWidget(input_group)
        
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
        self.contact_button = QPushButton("Связь с разработчиком: https://t.me/Userspoi")
        self.contact_button.clicked.connect(self.open_contact)
        contact_layout.addWidget(self.contact_button)
        main_layout.addLayout(contact_layout)
        
        # Parser result storage
        self.parser_result = None
        
        # Set example values
        self.domain_input.setText("ddx_fitness")
        self.owner_input.setText("-164992662")
        self.token_input.setText("e5381dcde5381dcde5381dcd27e60409cfee538e5381dcd8dc6434edf86e231157f87ee")
        
    def log_message(self, message):
        self.log_area.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
        
    def start_parsing(self):
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
        
        self.log_message("Начало парсинга...")
        
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
        
    def parsing_error(self, error_message):
        self.start_button.setEnabled(True)
        self.export_button.setEnabled(False)
        self.progress_bar.setValue(0)
        QMessageBox.critical(self, "Ошибка", f"Произошла ошибка при парсинге:\n{error_message}")
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
