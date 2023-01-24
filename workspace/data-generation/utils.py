import random
from datetime import datetime, timedelta
from dateutils import relativedelta

config = {
    "host": "postgres-b2b",
    "database": "b2b-platform",
    "user": "docker",
    "password": "docker"
}

def fetch_ids(conn, query):
    cursor = conn.cursor()
    cursor.execute(query)
    fetched = cursor.fetchall()
    ids_list = fetched
    if len(fetched[0]) == 1:
        ids_list = list(map(lambda x: x[0], fetched))
    cursor.close()
    return ids_list

def date_list(date: str, n: int, seed):
    random.seed(seed)
    tic = lambda: random.randint(0, 59)
    current = datetime.strptime(date, "%Y-%m-%d") - relativedelta(days = 1)
    first = datetime(current.year, current.month, 1)
    date_format = "%Y-%m-%d %H:%M:%S"
    return [datetime(first.year, first.month, random.randint(first.day, current.day), random.randint(0, 23), tic(), tic()).strftime(date_format) for i in range(0, n)]
