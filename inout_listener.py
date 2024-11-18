import yaml
import re
import os
import time
import ccxt
import shutil
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Чтение конфигурации из YAML
def load_config(config_path="conf.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config

# Класс для управления подключениями к биржам
class ExchangeManager:
    def __init__(self):
        self.exchanges = {}

    # Возвращаем подключение к бирже. Если оно уже существует, используется кеш
    def get_exchange(self, exchange_name, exchange_config):
        key = (exchange_name, exchange_config['api_key'])
        if key not in self.exchanges:
            try:
                # Динамическая инициализация биржи с использованием ccxt
                exchange_class = getattr(ccxt, exchange_name)
                self.exchanges[key] = exchange_class({
                    'apiKey': exchange_config['api_key'],
                    'secret': exchange_config['api_secret'],
                    'options': {'defaultType': 'future'}
                })
                print(f"Подключение к бирже {exchange_name} создано")
            except AttributeError:
                raise ValueError(f"Биржа с именем '{exchange_name}' не найдена в ccxt")
        return self.exchanges[key]

    # Получаем баланс с биржи
    def fetch_balance(self, exchange_name, exchange_config):
        try:
            exchange = self.get_exchange(exchange_name, exchange_config)
            balance = exchange.fetch_balance()
            return balance
        except Exception as e:
            print(f"Ошибка при получении баланса: {e}")
            return None

    # Получаем открытые позиции с биржи
    def fetch_positions(self, exchange_name, exchange_config):
        try:
            exchange = self.get_exchange(exchange_name, exchange_config)
            return exchange.fetch_positions()
        except Exception as e:
            print(f"Ошибка при получении позиций: {e}")
            return []

# Класс-обработчик событий
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, config, exchange_manager):
        super().__init__()
        self.config = config
        self.exchange_manager = exchange_manager

    def on_created(self, event):
        # Проверка, что это файл, и он добавлен в папку in
        if event.is_directory or not event.src_path.startswith(self.config["in"]):
            return

        print(f"Новый файл: {event.src_path}")
        self.process_catched_file(event.src_path)

    def check_active_positions(self, symbol, exchange_name, exchange_config):
        # Проверяет наличие активных позиций по символу
        positions = self.exchange_manager.fetch_positions(exchange_name, exchange_config)
        for position in positions:
            if position['symbol'] == symbol and position['contracts'] > 0:
                print(f"Открыта позиция по инструменту {symbol}.")
                return True
        print(f"Открытых позиций по инструменту {symbol} нет")
        return False

    def process_catched_file(self, file_path):
        # Задержка перед обработкой для избежания ошибки доступа
        time.sleep(1)

        # Флаг для отслеживания успешного перемещения файла
        file_moved = False

        # Получаем имя файла без пути
        file_name = os.path.basename(file_path)

        # Проход по списку директорий из nested
        for directory in self.config["nested"]:
            exchange_name = directory["exchange_name"]
            account_name = directory["account_name"]
            exchange_config = directory["exchange_config"]
            matching_pattern = directory["matching"]

            # 1. Проверяем, совпадает ли имя файла с регулярным выражением из matching
            if matching_pattern and re.match(matching_pattern, file_name):
                print(f"Файл {file_name} соответствует регулярному выражению {matching_pattern} "
                      f"для {directory['path']}")

                # 2. Проверяем баланс
                balance = self.exchange_manager.fetch_balance(exchange_name, exchange_config)

                if balance and balance.get('USDT', {}).get('total', 0) > 0:
                    print(f"Баланс положительный для {account_name} на {exchange_name}")

                    # 3. Извлекаем символ из первых 6 символов имени файла
                    symbol = file_name[:6]
                    if not self.check_active_positions(symbol, exchange_name, exchange_config):
                        print(f"Открытых позиций по инструменту из файла {file_name} нет, "
                              f"перемещаем файл в in")
                        destination_path = os.path.join(directory["path"], file_name)
                        shutil.move(file_path, destination_path)
                        print(f"Файл перемещен в {destination_path} на бирже {exchange_name}, аккаунт {account_name}")
                        file_moved = True
                        break
                    else:
                        print(f"Открыта позиция для {symbol}, файл {file_name} не перемещен")
                else:
                    print(f"Баланс для {account_name} на {exchange_name} равен 0")
            else:
                print(f"Файл {file_name} не соответствует шаблону {matching_pattern} для папки"
                      f" {directory['path']}")

        # Иначе перемещаем в out
        if not file_moved:
            out_path = os.path.join(self.config["out"], file_name)
            shutil.move(file_path, out_path)
            print(f"Файл не был перемещен в папки nested, поэтому перемещен в {out_path}")


# Основной запуск
def main():
    # Загружаем конфигурацию
    config = load_config()

    # Проверяем, что папка для отслеживания существует
    if not os.path.exists(config["in"]):
        print(f"Папки {config['in']} не существует")
        return

    # Настраиваем наблюдателя
    exchange_manager = ExchangeManager()
    event_handler = NewFileHandler(config, exchange_manager)
    observer = Observer()
    observer.schedule(event_handler, path=config["in"], recursive=False)
    observer.start()

    print(f"Отслеживание папки: {config['in']}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()