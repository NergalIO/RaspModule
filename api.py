from RaspModule.database import DatabaseManager
import RaspModule.dstu as dstu
import logging
import asyncio
import numpy

from asgiref import sync

FORMAT = '%(asctime)s - (%(levelname)s) [%(name)s] %(message)s'
DATEFMT = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt=DATEFMT)


class DstuAPI:
    def __init__(self, authToken: str) -> None:
        self.logger = logging.getLogger("RaspModule.DstuAPI")

        self.logger.info("Инициализация DstuAPI")
        self.token = authToken
        self.manager = dstu.DstuManager(self.token)

    def get_rasp(self) -> None:
        self.logger.info("Запрос для получения расписания")
        return self.manager.get_rasp()

    def get_all_commands(self) -> list[int]:
        self.logger.info("Запрос для получения списка команд")
        commands = []
        for lesson in self.get_rasp():
            command_ids = [command['groupID']
                           for command in lesson['info']['groups']]
            for command_id in command_ids:
                if command_id not in commands:
                    commands.append(command_id)
        return commands

    def get_students_from_commands(self, ids: list[int]) -> list[str]:
        return self.manager.get_students_from_commands(ids)

    def get_students_from_command(self, id: str) -> list[str]:
        return self.manager.get_students_from_command(id)


class DatabaseAPI:
    def __init__(self, dumps_folder: str) -> None:
        self.logger = logging.getLogger("RaspModule.DatabaseAPI")

        self.logger.info("Инициализация базы данных")
        self.database = DatabaseManager(
            "RaspManager",
            {
                "lessons": {
                    "name": "Lessons",
                    "values": "name String, id String, teacher String, auditory String, start_time String, end_time String, groups Array(String)",
                    "insert": "name, id, teacher, auditory, start_time, end_time, groups"
                },
                "students": {
                    "name": "Students",
                    "values": "id String, fullname String",
                    "insert": "id, fullname"
                },
                "groups": {
                    "name": "Groups",
                    "values": "name String, students Array(String)",
                    "insert": "name, students"
                }
            },
            dumps_folder
        )

    def insert_group(self, name: str, students: list[str]) -> None:
        self.logger.debug(
            f"Добавление группы -> name: {name}, students-count: {len(students)}"
        )
        self.database.insert("groups", f"'{name}', {students}")

    def insert_lesson(self, name: str, id: str, teacher: str, auditory: str, start_time: str, end_time: str, groups: list[str]) -> None:
        self.logger.debug(f"Добавление урока -> name: {name}, id: {id}")
        self.database.insert(
            "lessons", f"'{name}', '{id}', '{teacher}', '{auditory}', '{start_time}', '{end_time}', {groups}"
        )

    def get_rasp_for_user(self, id: str, day: int = 0) -> None:
        fullname = self.get_user(id)

        user_groups = []
        for id, students in self.database.select('groups'):
            if fullname in students:
                user_groups.append(id)

        if day == 0:
            date = dstu.date.get_db_time("").split("T")[0]
        else:
            date = dstu.date.get_db_time_at_day(day, "").split("T")[0]
        
        lessons = self.database.select('lessons')
        async def usage(ids):
            async def fetch(id):
                user_lessons = []
                for lesson in lessons:
                    if lesson[4].split("T")[0] != date:
                        continue
                    if id in lesson[6]:
                        user_lessons.append(lesson)
                return user_lessons
            return await asyncio.gather(*[
                fetch(id) for id in ids
            ])
            
        result = []
        for lesson in sync.async_to_sync(usage)(user_groups):
            if lesson in result or len(lesson) == 0:
                continue
            result += lesson
        return result

    def check_user_in_groups(self, fullname: str) -> bool:
        for id, students in self.database.select('groups'):
            if fullname in students:
                return True
        return False

    def register_user(self, id: str, fullname: str) -> None:
        if not self.check_user(id):
            self.logger.debug(f"Регистрация пользователя -> id: {id}")
            self.database.insert("students", f"'{id}', '{fullname}'")

    def get_user(self, id: str) -> tuple[str, str] | None:
        self.logger.info(f"Запрос на получения данных пользователя {id}")
        if not self.check_user(id):
            return None
        return self.database.select("students", f"id='{id}'")[0][1]

    def check_user(self, id: str) -> bool:
        self.logger.info(
            f"Проверка существования пользователя в базе данных id: {id}")
        students = self.database.select("students", f"id='{id}'")
        return len(students) != 0

    def delete_item(self, table: str, key: str, value: str) -> None:
        self.logger.info(
            f"Удаления строки данных в таблице {table}, где {key}='value'"
        )
        self.database.delete(table, f"{key}='{value}'")

    def clear_table(self, table: str) -> None:
        self.logger.info(f"Удаление всех данных из таблицы {table}")
        self.database.clear_table(table)

    def load(self, backup_name: str = "last_backup") -> None:
        self.logger.info(f"Загрузка бекапа {backup_name}")
        self.database.load_backup(backup_name)

    def save(self) -> None:
        backup_name = dstu.date.get_db_name_time()
        self.logger.info(f"Создание бекапа в {backup_name}.json")
        self.database.save_backup(backup_name)

    def close(self) -> None:
        self.logger.info(f"Закрытие базы данных")
        self.save()
        del self
