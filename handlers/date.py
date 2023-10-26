import datetime


def _get_db_time(year: str, month: str, day: str, time: str) -> str:
    return f"{year}-{month}-{day}T{time}:00+03:00"


def _get_db_name_time(year: str, month: str, day: str, time: str) -> str:
    return f"{year}-{month}-{day}T{time}"


def get_db_name_time() -> str:
    return _get_db_name_time(get_current_year(), get_month(), get_day(), get_time())


def get_db_time(time: str) -> str:
    return _get_db_time(get_current_year(), get_month(), get_day(), time)


def get_db_time_at_day(day: str, time: str) -> str:
    return _get_db_time(get_current_year(), get_month(), day, time)


def get_time() -> str:
    time = datetime.datetime.now()
    return f"{time.hour}:{time.minute}:{time.second}"


def get_day() -> str:
    return str(datetime.datetime.now().day)


def get_month() -> str:
    return str(datetime.datetime.now().month)


def get_current_year() -> str:
    return str(datetime.datetime.now().year)


def get_year() -> str:
    current_year = get_current_year()
    return f"{current_year}-{int(current_year) + 1}"
