import yaml
import os
import time
import random
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

    def process_catched_file(self, file_path):
        # Задержка перед обработкой для избежания ошибки доступа
        time.sleep(1)

        # Флаг для отслеживания успешного перемещения файла
        file_moved = False

        # Проход по списку директорий из nested
        for directory in self.config["nested"]:
            random_number = round(random.uniform(0, 1), 3)
            print(f"Сгенерировано случайное число: {random_number} для директории {directory['path']}")

            # Если случайное число больше 0.5, перемещаем файл в in и выходим из цикла
            if random_number > 0.5:
                destination_path = os.path.join(directory["path"], os.path.basename(file_path))
                shutil.move(file_path, destination_path)
                print(f"Файл перемещен в {destination_path}")

                exchange_name = directory["exchange_name"]
                exchange_config = directory["exchange_config"]
                print(f"Path: {directory['path']}, Exchange Name: {exchange_name}, Exchange "
                      f"Config: {exchange_config}")

                file_moved = True
                break

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