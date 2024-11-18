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


# Класс-обработчик событий
class NewFileHandler(FileSystemEventHandler):
    def __init__(self, config):
        super().__init__()
        self.config = config

    def on_created(self, event):
        # Проверка, что это файл, и он добавлен в папку in
        if event.is_directory or not event.src_path.startswith(self.config["in"]):
            return

        print("Added")
        print(f"Новый файл: {event.src_path}")

        # Обработка файла:
        self.process_catched_file(event.src_path)

    def connect_and_fetch_balance(self, exchange_name, account_name, exchange_config):
        try:
            # Динамическая инициализация биржи с использованием ccxt
            exchange_class = getattr(ccxt, exchange_name)
            exchange = exchange_class({
                'apiKey': exchange_config['api_key'],
                'secret': exchange_config['api_secret'],
                'options': {'defaultType': 'future'}
            })

            # Получение баланса
            balance = exchange.fetch_balance()
            print(f"Баланс для аккаунта {account_name} на бирже {exchange_name}: {balance['total']}")

            # Возвращаем баланс
            return balance['total']

        except AttributeError:
            print(f"Биржа с именем '{exchange_name}' не найдена в ccxt.")
        except Exception as e:
            print(f"Ошибка при подключении к бирже {exchange_name} для аккаунта {account_name}: {str(e)}")
        return None

    def check_active_positions(self, file_name, exchange_name, account_name, exchange_config):

        # Компилируем регулярное выражение
        pattern = re.compile(r"^(?P<symbol>[A-Z]+USDT)\+.*")

        # Извлекаем символ из имени файла
        match = pattern.match(file_name)
        if not match:
            print(f"Не удалось извлечь символ из имени файла {file_name}, пропускаем")
            return False

        symbol = match.group("symbol")
        print(f"Проверяем наличие открытой позиции по инструменту {symbol} для аккаунта {account_name} на бирже {exchange_name}")

        try:
            # Динамическая инициализация биржи с использованием ccxt
            exchange_class = getattr(ccxt, exchange_name)
            exchange = exchange_class({
                'apiKey': exchange_config['api_key'],
                'secret': exchange_config['api_secret'],
                'options': {'defaultType': 'future'}
            })

            # Получаем активные позиции
            positions = exchange.fetch_positions()
            for position in positions:
                if position['symbol'] == symbol and position['contracts'] > 0:
                    print(f"Открыта позиция по инструменту {symbol}, пропускаем файл")
                    return True

            print(f"Открытых позиций по инструменту {symbol} нет")
            return False

        except Exception as e:
            print(f"Ошибка при проверке открытых позиций: {str(e)}")
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
                print(f"Файл {file_name} соответствует регулярному выражению {matching_pattern} для папки {directory['path']}")

                # Подключаемся к бирже и проверяем баланс
                balance = self.connect_and_fetch_balance(exchange_name, account_name, exchange_config)

                # 2. Проверяем, есть ли положительный баланс USDT
                if balance and balance.get('USDT', 0) > 0:
                    print(f"Баланс положительный для аккаунта на {account_name} на "
                          f"бирже {exchange_name}, "
                          f"перемещаю файл в папку {directory['path']}")

                    # 3. Проверяем, что позиция не открыта
                    if not self.check_active_positions(file_name, exchange_name, account_name,
                                                  exchange_config):
                        print(f"Открытых позиций по инструменту из файла {file_name} нет, "
                              f"перемещаем файл в in")
                        destination_path = os.path.join(directory["path"], os.path.basename(file_path))
                        shutil.move(file_path, destination_path)
                        print(f"Файл перемещен в {destination_path} на бирже {exchange_name}, аккаунт {account_name}")
                        file_moved = True
                        break
                    else:
                        print(f"Открыта позиция по инструменту из файла {file_name}, следующая итерация")
                else:
                    print(f"Баланс для аккаунта {account_name} на бирже {exchange_name} равен 0, "
                          f"следующая итерация")
            else:
                print(f"Файл {file_name} не совпадает с matching {matching_pattern} для папки"
                      f" {directory['path']}, следующая итерация")


        # for directory in self.config["nested"]:
        #     random_number = round(random.uniform(0, 1), 3)
        #     print(f"Сгенерировано случайное число: {random_number} для директории {directory['path']}")
        #
        #     # Если случайное число больше 0.5, перемещаем файл в in и выходим из цикла
        #     if random_number > 0.5:
        #         destination_path = os.path.join(directory["path"], os.path.basename(file_path))
        #         shutil.move(file_path, destination_path)
        #         print(f"Файл перемещен в {destination_path}")
        #
        #         # Подключение к бирже и вывод баланса
        #         self.connect_and_fetch_balance(directory['exchange_name'],
        #                                        directory['account_name'],
        #                                        directory['exchange_config'])
        #
        #         file_moved = True
        #         break

        # Иначе перемещаем в out
        if not file_moved:
            out_path = os.path.join(self.config["out"], os.path.basename(file_path))
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
    event_handler = NewFileHandler(config)
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