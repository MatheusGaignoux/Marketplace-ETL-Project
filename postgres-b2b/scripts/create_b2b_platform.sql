-- companies() (domain):
-- CompanyId (CUIT), CompanyName
drop table if exists companies;
create table companies (
    CompanyId serial primary key,
    CompanyName text not null,
    IsSupplier boolean not null
)
;

-- products() (domain):
-- ProductId, ProductName
drop table if exists products;
create table products (
    ProductId serial primary key,
    ProductName text not null
)
;

-- customers() (domain):
-- CustomerId, CustomerName
drop table if exists customers;
create table customers (
    CustomerId serial primary key,
    CustomerName text not null
)
;

-- catalog(companies{1}, products{1}) (domain):
-- CatalogId, ProductId, CompanyId, SupplierId, Price
drop table if exists catalog;
create table catalog (
    CatalogId serial primary key,
    ProductId integer not null,
    CompanyId integer not null,
    SupplierId integer,
    Price float not null,
    constraint ProductId
        foreign key (ProductId)
        references products(ProductId),
    constraint CompanyId
        foreign key (CompanyId)
        references companies(CompanyId),
    constraint SupplierId
        foreign key (SupplierId)
        references companies(CompanyId)
)
;

create unique index ProductCompanyId on catalog (CompanyId, ProductId);

-- orders(customers{1}) (event):
-- OrderId, CustomerId, OrderDate
drop table if exists orders;
create table orders (
    OrderId serial primary key,
    CustomerId integer not null,
    OrderDate timestamp not null,
        constraint CustomerId
            foreign key (CustomerId)
            references customers(CustomerId)
)
;

-- orderItems(orders{1}, catalog{n}) (event):
-- OrderItemId, OrderId, CatalogId
drop table if exists orderItems;
create table orderItems (
    OrderItemId serial primary key,
    OrderId integer not null,
    CatalogId integer not null,
        constraint OrderId
            foreign key (OrderId)
            references orders(OrderId),
        constraint CatalogId
            foreign key (CatalogId)
            references catalog(CatalogId)
)
;

drop table if exists ipsrangelist;
create table ipsrangelist (
    country text not null,
    IpRange text not null
)
;

-- Importing data
copy customers (CustomerName)
from '/project/data/customers.csv'
delimiter ','
;

copy products (ProductName)
from '/project/data/products.csv'
delimiter ','
;

copy companies (CompanyName, IsSupplier)
from '/project/data/companies.csv'
delimiter ','
;

copy ipsrangelist (country, IpRange)
from '/project/data/ips_range_list.csv'
delimiter ','
;