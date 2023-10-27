import RaspModule.handlers.date as date
import requests
import aiohttp
import asyncio

from asgiref import sync

def fetch_json(url: str, headers: dict = None, *args, **kwargs) -> dict:
    return requests.get(url, headers=headers, timeout=20, *args, **kwargs).json()


class DstuManager:
    def __init__(self, authToken: str) -> None:
        self.authToken = authToken

    def get_rasp(self) -> dict:
        return fetch_json(
            f"https://edu.donstu.ru/api/RaspManager?educationSpaceId=4&month={date.get_month()}&showJournalFilled=false&year={date.get_year()}&showAll=true",
        )['data']['raspList']

    def get_groups(self) -> list:
        return fetch_json(
            f"https://edu.donstu.ru/api/raspGrouplist?year={date.get_year()}",
        )['data']

    def get_students_from_group(self, id: int) -> list:
        return fetch_json(
            f"https://edu.donstu.ru/api/UserInfo/GroupInfo?groupID={id}",
        )
    
    def get_students_from_commands(self, ids: list[int]) -> list[list[str]]: 
        urls = [f"https://edu.donstu.ru/api/GroupManager/StudentsExport?groupID={id}" for id in ids]
        async def get_all(urls):
            async with aiohttp.ClientSession() as session:
                async def fetch(url):
                    async with session.get(url, headers={"Cookie": f"authToken={self.authToken}"}) as response:
                        return [url.split("=")[1], (await response.json())['data']['students']]
                return await asyncio.gather(*[
                    fetch(url) for url in urls
                ])
        return sync.async_to_sync(get_all)(urls)


    def get_students_from_command(self, id: int) -> list[str]:
        return [student['name'] for student in fetch_json(
            f"https://edu.donstu.ru/api/GroupManager/StudentsExport?groupID={id}",
            headers={"Cookie": f"authToken={self.authToken}"}
        )['data']['students']]

    def get_all_commands(self) -> list[int]:
        commands = []
        for lesson in self.get_rasp():
            command_ids = [command['groupID']
                           for command in lesson['info']['groups']]
            for command_id in command_ids:
                if command_id not in commands:
                    commands.append(command_id)
        return commands
