import RaspModule.api as api
import threading
import logging
import json
import time

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

        self._update_groups()
    
        self.logger.info("Запуск потока обновления расписания")
        try:
            self.update_rasp_thread = threading.Thread(
                target=self._update_rasp,
                args=(self.config['raspUpdateInterval'], ))
            self.update_rasp_thread.start()
        except Exception as error:
            self.logger.error(f'При запуске потока произошла ошибка: {error}')
            exit(1)

        #self.logger.info("Запуск потока обновления команд")
        #try:
        #    self.update_groups_thread = threading.Thread(
        #        target=self._update_groups,
        #        args=(self.config['groupsUpdateInterval'], ))
        #    self.update_groups_thread.start()
        #except Exception as error:
        #    self.logger.error(f'При запуске потока произошла ошибка: {error}')
        #    exit(1)
        

        self.logger.info("Запуск потока автомотического бекапа базы данных")
        try:
            self.update_rasp_thread = threading.Thread(
                target=self._auto_backup,
                args=(self.config['autoBackupInterval'], ))
            self.update_rasp_thread.start()
        except Exception as error:
            self.logger.error(f'При запуске потока произошла ошибка: {error}')
            exit(1)
        self.logger.info("Модуль расписания успешно запущен!")

    def get_rasp_for_student(self, id: str, day: int = 0) -> list[tuple]:
        return self.database.get_rasp_for_user(id, day)

    def check_user_in_groups(self, fullname: str) -> bool:
        return self.database.check_user_in_groups(fullname)

    def register_user(self, id: str, fullname: str) -> None:
        self.database.register_user(id, fullname)
    
    def check_user(self, id: str) -> bool:
        return self.database.check_user(id)

    def _auto_backup(self, interval: int) -> None:
        while True:
            time.sleep(interval)
            self.database.save()

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
            self.database.save()
            time.sleep(interval)

    def _update_groups(self) -> None:
        self.logger.info("Обновление команд...")
        try:
            commands = self.dstu.get_all_commands()

            for id, students in self.dstu.get_students_from_commands(commands):
                students = [student['name'] for student in students]
                self.database.insert_group(id, students)

            self.logger.info("Команды обновлены!")
        except Exception as error:
            self.logger.warning(
                f"Неудалось обновить список команд! Ошибка: {error}")


if __name__ == "__main__":
    rasp = RaspManager("RaspModule/config.json")
