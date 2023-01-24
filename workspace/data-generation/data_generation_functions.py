import random
import itertools
import psycopg2 as psql
import ipaddress

from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from math import floor
from fake_useragent import UserAgent
from utils import date_list

def choice_ip(boundary_ips):
    start = ipaddress.ip_address(boundary_ips[0])
    end = ipaddress.ip_address(boundary_ips[1])
    ip = start
    if start != end:
        ip = random.choice([ipaddress.ip_address(i).exploded for i in range(int(start), int(end))])
    return ip
    
def sample_ips(iprangelist, k, seed):
    random.seed(seed)
    iprange_sample = random.choices([iprange for iprange in iprangelist], k = k)
    boundary_ips = list(map(lambda iprange: iprange.split("-"), iprange_sample))
    return list(map(choice_ip, boundary_ips))

def batch_order_items(conn, date, customers, catalog, iprangelist, seed):
    random.seed(seed)
    # The number of lines to be inserted needs to have an up limit (100) once the returning clause in postgresql limits the answer of a query to only 100 rows.
    n = random.randint(50, 100)
    orders = []
    
    for batch in range(0, n):
        k = random.randint(1, 10)
        # Choosing a number of items to compose the order
        items_choices = random.choices(catalog, k = k)
        cat_items = []
        prods = []
        for i in items_choices: cat_items.append(i[0]); prods.append(i[1])
        items = [cat_items, list(set(prods))]
        # Choosing the customer who will make the order
        customer = random.choice(customers)       
        orders += [(customer, items)]
    
    orders = list(map(lambda x: (x[0][0][0], x[0][0][1], x[0][1], x[1]), zip(orders, date_list(date, n, seed))))

    sql_orders = "insert into orders (CustomerId, OrderDate) values %s returning OrderId"

    cursor = conn.cursor()
    execute_values(cursor, sql_orders, map(lambda x: (x[0], x[3]), orders))
    orders_id = list(map(lambda x: x[0], cursor.fetchall()))

    # After the insert, the database will return the new orderIds generated.
    # Before one organizes all the information, it is necessary to generate a list with possible values of ip:
    ips = sample_ips(iprangelist, n, seed)
    # Then, one redefines the orders infos as a tupple of: (CustomerId, CustomerName, items [CatalogId][ProductId], OrderDate, OrderId, Ip):
    orders = list(map(lambda x: (x[0][0], x[0][1], x[0][2], x[0][3], x[1], x[2]), zip(orders, orders_id, ips)))
    
    sql_orders_items = "insert into orderItems (OrderId, CatalogId) values %s"
    # The items field in the order list needs to exploded, so one can have an associated orderItemId for each combination of (orderId, catalogId):
    order_items = list(itertools.chain.from_iterable([itertools.product([x[4]], x[2][0]) for x in orders]))
    execute_values(cursor, sql_orders_items, order_items)
    
    conn.commit()
    cursor.close()
    
    return orders

def prepare_order_weblogs(seed):
    def order_weblogs(order):
        random.seed(seed)
        user_agent = UserAgent()
        page_useragent = user_agent.random

        orderdate = datetime.strptime(order[3], "%Y-%m-%d %H:%M:%S")
        visited_at = lambda date: (date - timedelta(minutes = random.randint(0, 30), seconds = random.randint(0, 59))).strftime("%Y-%m-%d %H:%M:%S")

        product_weblog = lambda x: f'{order[5]} - "{order[1].replace(" ", ".").lower()}" {visited_at(orderdate)} - "200" - "http://mktplace.toptal.com/products/{x}" "{page_useragent}"'

        customer_products = list(map(product_weblog, order[2][1]))

        customer_order = f'{order[5]} - {order[1].replace(" ", ".").lower()} {order[3]} - "200" - "http://mktplace.toptal.com/orders/{order[4]}" "{page_useragent}"'

        return customer_products + [customer_order]
    return order_weblogs

def visits_weblogs(date, customersNames, products, iprangelist, n, seed):
    random.seed(seed)
    user_agent = UserAgent()
    rand_products = random.choices(products, k = n)
    rand_customers = random.choices(customersNames + list(itertools.repeat("unknown", len(customersNames))), k = n)
    
    weblog = lambda x: f'{x[0]} - {x[1].replace(" ", ".").lower()} {x[2]} - "200" - "http://mktplace.toptal.com/products/{x[3]}" "{user_agent.random}"'
    
    return list(map(weblog, zip(sample_ips(iprangelist, n, seed), rand_customers, date_list(date, n, seed), rand_products)))

