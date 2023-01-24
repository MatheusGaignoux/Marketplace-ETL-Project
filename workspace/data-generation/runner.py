import argparse

from datetime import datetime
from utils import config, fetch_ids, date_list
from data_generation_functions import *
from pathlib import Path

def run(execution_date):
    """
        Generation of data for logs + feeding of the database.
    """
    conn = psql.connect(**config)

    ensure_length = lambda x: str(x) if len(str(x)) % 2 == 0 else "0" + str(x)
    
    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    year = ensure_length(dt.year)
    month = ensure_length(dt.month)
    seed = int(year + month)
    
    query_catalogs  = "select distinct CatalogId, ProductId from catalog order by ProductId, CatalogId"
    query_customers = "select distinct CustomerId, CustomerName from customers order by CustomerId"
    query_ipsrange = "select IpRange from ipsrangelist"

    catalog = fetch_ids(conn, query_catalogs)
    customers = fetch_ids(conn, query_customers)
    ipsrangelist = fetch_ids(conn, query_ipsrange)

    orders = batch_order_items(conn, execution_date, customers, catalog, ipsrangelist, seed)

    order_weblogs = prepare_order_weblogs(seed)
    orders_logs = list(itertools.chain.from_iterable(map(order_weblogs, orders)))
    
    query_customernames = "select CustomerName from customers"
    query_products = "select ProductId from products order by ProductId"
    customerNames = fetch_ids(conn, query_customernames)
    products = fetch_ids(conn, query_products)
    visits_logs = visits_weblogs(execution_date, customerNames, products, ipsrangelist, 100, seed)
    
    logs = orders_logs + visits_logs

    conn.close()

    log_path = f"/b2b-platform/data/logs/year={year}/month={month}"
    file_name = "file.log"
    Path(log_path).mkdir(parents = True, exist_ok = True)
    print(f"Saving as file: {log_path}/{file_name}")
    with open(f"{log_path}/{file_name}", "a+") as file:
        for line in logs:
            file.write(f"{line}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-d", "--ExecutionDate", type = str)
    args = parser.parse_args()
    execution_date = args.ExecutionDate
    run(execution_date)
