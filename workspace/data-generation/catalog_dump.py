import random
import itertools
import psycopg2 as psql

from psycopg2.extras import execute_values
from utils import config, fetch_ids

def run_catalog_dump(conn, products, suppliers, companies) -> None:
    """
        This function was designed to feed the table catalog in the b2b-platform database once the creation
        of its dependencies (the tables products and companies). We bear with the following two accomplishments:

        (i)  Generate every pair combinations of products and suppliers, each one with a correspondent price defined as:
             supplier_price = product_default_price + supplier_increment.

        (ii) Generate records with all the fields. Each company catalog list does not necessarily has all the products provided by each supplier.
             The company price for each product supplied follows a similar formula from the last one:
             company_price = supplier_price + company_increment.
    """
    
    random.seed(42)

    products_infos = {x: [] for x in products}
    # Defining a default price for each unique ProductId:
    products_default_price = list(map(lambda x: (x, random.randrange(10, 1000, step = 5) + random.choice([.5, .2, .9])), products))

    # The next steps bears in the creating of two types of records following the catalog table schema: the suppliers records and the companies records.
    # Companies have a supplier or not. When not, it is named as a supplier. That is why the suppliers records needs to be contructed first 
    # once the catalog table has a self referencing in the keys CompanyId and SupplierId.   

    suppliers_rows = []
    companies_rows = []

    for supplier in suppliers:
        k = random.randint(1, len(products))
        price_increment = random.randrange(1, 10, step = 1)
        # Select only the products that will supplied by the current supplier:
        selected_products = itertools.product([supplier], random.sample(products_default_price, k))
        # Formatting the rows to be inserted in table:
        supplier_rows = list(map(lambda x: (x[1][0], x[0], None, x[1][1] + price_increment), selected_products))
        suppliers_rows += supplier_rows

    for row in suppliers_rows:
        # For each key (ProductId): gives a list of key value formatted as (supplier company id, product price given the specific supplier)
        products_infos[row[0]].append([row[1], row[3]])

    for company in companies:
        k = random.randint(1, len(products))
        price_increment = random.randrange(1, 30, step = 1)
        # Choosing the products which will be sold by the current company:
        products_to_sell = random.sample(products, k)
        # Finding from which suppliers to buy the choosen products:
        from_supplier = [[x] + random.choice(products_infos[x]) for x in products_to_sell]
        # Formatting the rows to be inserted in table:
        company_row = list(map(lambda x: (x[0], company, x[1], x[2] + price_increment), from_supplier))   
        companies_rows += company_row
        
    rows = suppliers_rows + companies_rows

    sql = """
        insert into catalog (ProductId, CompanyId, SupplierId, Price)
        values %s 
        on conflict (CompanyId, ProductId) do 
        update set Price = catalog.Price
    """

    cursor = conn.cursor()
    execute_values(cursor, sql, rows)
    conn.commit()
    cursor.close()

if __name__ == "__main__":
    conn = psql.connect(**config)

    query_products  = "select distinct ProductId  from products order by ProductId"
    query_suppliers = "select distinct CompanyId from companies where isSupplier = true order by CompanyId"
    query_companies = "select distinct CompanyId from companies where isSupplier = false order by CompanyId"

    products  = fetch_ids(conn, query_products)
    suppliers = fetch_ids(conn, query_suppliers)
    companies = fetch_ids(conn, query_companies)

    run_catalog_dump(conn, products, suppliers, companies)

    conn.close()