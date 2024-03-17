-- !preview conn=DBI::dbConnect(RSQLite::SQLite())

CREATE TABLE CUSTOMER (
"CUSTOMER_ID" int primary key,
"CUSTOMER_EMAIL" varchar(320) not null,
"CUSTOMER_PHONE_NUMBER" varchar(20) not null,
"CUSTOMER_FIRST_NAME" varchar(40) not null,
"CUSTOMER_MIDDLE_NAME" varchar(40) null,
"CUSTOMER_LAST_NAME" varchar(40) not null,
"CUSTOMER_DOB" date not null,
"CUSTOMER_GENDER" varchar(10) not null,
"CUSTOMER_ADDRESS_ID" int,
foreign key ("CUSTOMER_ADDRESS_ID")
  references ADDRESS("CUSTOEMR_ADDRESS_ID")
);

create table CUSTOMER_ADDRESS(
"CUSTOMER_ADDRESS_ID" int primary key,
"CUSTOMER_ADDRESS_NUMBER" int not null,
"CUSTOMER_STREET" varchar(40) not null,
"CUSTOMER_POSTCODE" int not null,
"CUSTOMER_CITY" varchar(20) not null,
"COUNTRY_ID" varchar(10),
foreign key ("COUNTRY_ID") 
  references COUNTRY("COUNTRY_ID")
);

create table COUNTRY(
"COUNTRY_ID" varchar(10) primary key,
"COUNTRY_NAME" varchar(20) not null
);

CREATE TABLE ORDERS(
"ORDER_ID" int,
"CUSTOMER_ID" int,
"PRODUCT_ID" int,
"ORDER_ITEM_QTY" int not null,
"REVIEW_RATING" int null,
PRIMARY KEY ("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID"),
foreign key ("CUSTOMER_ID")
  references CUSTOMER("CUSTOMER_ID")
foreign key ("PRODUCT_ID")
  references PRODUCT("PRODUCT_ID")
);


CREATE TABLE ORDER_DETAIL(
"ORDER_ID" int,
"ORDER_STATUS_ID" int,
"PURCHASE_DATE" date not null,
"PAYMENT_METHOD_ID" int,
foreign key ("ORDER_STATUS_ID") 
  references ORDER_STATUS("ORDER_STATUS_ID")
foreign key ("PAYMENT_METHOD_ID")
  references PAYMENT_METHOD("PAYMENT_METHOD_ID")
foreign key ("ORDER_ID")
  references ORDERS("ORDER_ID")
);

create table ORDER_STATUS(
"ORDER_STATUS_ID" int primary key,
"ORDER_STATUS_NAME" varchar(40) not null
);

create table PRODUCT(
"PRODUCT_ID" int primary key,
"PRODUCT_CATEGORY_ID" int,
"SUPPLIER_ID" int,
"PRODUCT_NAME" varchar(30) not null,
"PRODUCT_DESCRIPTION" varchar(100) null,
"PRODUCT_PRICE" int not null,
"PRODUCT_QTY_AVAILABLE" int not null,
foreign key ("PRODUCT_CATEGORY_ID")
  references CATEGORY("PRODUCT_CATEGORY_ID")
foreign key ("SUPPLIER_ID")
  references SUPPLIER("SUPPLIER_ID")
);

create table PRODUCT_CATEGORY(
"PRODUCT_CATEGORY_ID" int primary key,
"PARENT_CATEGORY_ID" int,
"CATEGORY_NAME" varchar(40) not null,
foreign key ("PARENT_CATEGORY_ID") 
  references CATEGORY("PRODUCT_CATEGORY_ID")
);

create table SUPPLIER(
"SUPPLIER_ID" int primary key,
"SUPPLIER_FIRST_NAME" varchar(40) not null,
"SUPPLIER_MIDDLE_NAME" varchar(40) null,
"SUPPLIER_LAST_NAME" varchar(40) not null,
"SUPPLIER_ADDRESS_ID" int,
"SUPPLIER_EMAIL" varchar(40) not null,
"SUPPLIER_PHONE" varchar(20) not null,
"SUPPLIER_AGREEMENT" numeric not null,
foreign key ("SUPPLIER_ADDRESS_ID") 
  references SUPPLIER_ADDRESS("SUPPLIER_ADDRESS_ID")
);

create table SUPPLIER_ADDRESS(
"SUPPLIER_ADDRESS_ID" int primary key,
"SUPPLIER_POSTCODE" int not null,
"SUPPLIER_CITY" varchar(40) not null,
"COUNTRY_ID" varchar(10),
foreign key ("COUNTRY_ID")
  references COUNTRY("COUNTRY_ID")
);

create table SHIPPING(
"SHIPPING_ID" int primary key,
"ORDER_ID" int,
"SHIPPING_NAME" varchar(40) not null,
"DISPATCH_DATE" date null,
"DELIVERY_DATE" date null,
foreign key ("ORDER_ID")
  references ORDERS("ORDER_ID")
);

create table ADS(
"AD_ID" int primary key,
"PRODUCT_ID" int,
"ADS_DESCRIPTION" varchar(50) not null,
"DISCOUNT_RATE" numeric not null,
"ADS_START_DATE" date not null,
"ADS_END_DATE" date not null,
foreign key ("PRODUCT_ID")
  references PRODUCT("PRODUCT_ID")
);

create table PAYMENT_METHOD(
"PAYMENT_METHOD_ID" int primary key,
"PAYMENT_METHOD_NAME" varchar(50) not null
);


