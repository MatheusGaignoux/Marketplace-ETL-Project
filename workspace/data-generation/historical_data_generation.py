from datetime import datetime
from dateutils import relativedelta
from runner import *

if __name__ == "__main__":
    start_year = 2017
    date_diff = relativedelta(datetime.today() - relativedelta(months = 1), datetime(start_year, 1, 1))
    n = date_diff.years * 12 + date_diff.months

    for i in range(0, n + 1):
        date = datetime(start_year, 1, 1) + relativedelta(months = i)
        run(date.strftime("%Y-%m-%d"))
