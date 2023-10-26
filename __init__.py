import threading
import logging
import json
import time
import api

FORMAT = '%(asctime)s - (%(levelname)s) [%(name)s] %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt=DATEFMT)


class RaspManager:
    def __init__(self, config: str) -> None:
        self.logger = logging.getLogger("RaspModule.RaspManager")

        self.logger.info("Инициализация модуля расписаний...")
        self.logger.info("Загрузка конфига...")
        try:
            self.config = json.load(open(config, "r"))
        except Exception as error:
            self.logger.error(f"Ошибка при попытке загрузки конфига: {error}")
            exit(1)
        self.logger.info("Конфиг загружен!")

        try:
            self.database = api.DatabaseAPI(self.config['backup-folder'])
        except Exception as error:
            self.logger.error(
                f"Произошла ошибка при подключении к базе данных: {error}"
            )
            exit(1)

        try:
            self.dstu = api.DstuAPI(self.config['authToken'])
        except Exception as error:
            self.logger.error(
                f"Произошла ошибка при инициализации DstuAPI"
            )
            exit(1)

        self.logger.info("Запуск потока обновления расписания")
        try:
            self.update_rasp_thread = threading.Thread(
                target=self._update_rasp,
                args=(self.config['raspUpdateInterval'], ))
            self.update_rasp_thread.start()
        except Exception as error:
            self.logger.error(f'При запуске потока произошла ошибка: {error}')
            exit(1)

        self.logger.info("Запуск потока обновления команд")
        try:
            self.update_groups_thread = threading.Thread(
                target=self._update_groups,
                args=(self.config['groupsUpdateInterval'], ))
            self.update_groups_thread.start()
        except Exception as error:
            self.logger.error(f'При запуске потока произошла ошибка: {error}')
            exit(1)
        self.logger.info("Модуль расписания успешно запущен!")

        self.logger.info("Запуск потока автомотического бекапа базы данных")
        try:
            self.update_rasp_thread = threading.Thread(
                target=self._auto_backup,
                args=(self.config['autoBackupInterval'], ))
            self.update_rasp_thread.start()
        except Exception as error:
            self.logger.error(f'При запуске потока произошла ошибка: {error}')
            exit(1)

    def get_rasp_for_student(self, id: str, day: int = 0) -> list[tuple]:
        return self.database.get_rasp_for_user(id, day)

    def _auto_backup(self, interval: int) -> None:
        while True:
            self.database.save()
            time.sleep(interval)

    def _update_rasp(self, interval: int) -> None:
        while True:
            self.logger.info("Обновление расписания...")

            try:
                rasp = self.dstu.get_rasp()

                missed = 0
                self.database.clear_table("lessons")
                for lesson in rasp:
                    try:
                        groups = [
                            f'{item["groupID"]}' for item in lesson['info']['groups']]
                        teacher = 'None' if lesson['info']['teachersNames'] == '' else lesson['info']['teachersNames']
                        auditory = lesson["info"]["aud"]
                        name = lesson["name"].replace("'", " ")
                        id = lesson["info"]["raspItemID"]
                        start = lesson["start"]
                        end = lesson["end"]

                        self.database.insert_lesson(
                            name, id, teacher, auditory, start, end, groups
                        )
                    except:
                        self.logger.warning(
                            f"При попытке добавить предмет ({lesson['info']['raspItemID']}) произошла ошибка, пропуск добавления"
                        )
                self.logger.info(f"Расписание обновлено! Пропущено: {missed}")
            except Exception as error:
                self.logger.warning(
                    f"При обновлении расписании произошла ошибка: {error}"
                )
            time.sleep(interval)

    def _update_groups(self, interval: int) -> None:
        while True:
            self.logger.info("Обновление команд...")
            try:
                commands = self.dstu.get_all_commands()

                data = {}
                for id in commands:
                    students = self.dstu.get_students_from_command(id)
                    data[id] = students

                self.database.clear_table("groups")
                for id, students in data.items():
                    self.database.insert_group(id, students)

                self.logger.info("Команды обновлены!")
            except Exception as error:
                self.logger.warning(
                    f"Неудалось получить новый список команд! Ошибка: {error}")

            time.sleep(interval)


if __name__ == "__main__":
    RaspManager("config.json")
