library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Build connections
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "Database/sample.db")

# Read in csv file to dataframe
category_df <- readr::read_csv("Data_Upload/CATEGORY.csv")
product_df <- readr :: read_csv("Data_Upload/PRODUCT.csv")
country_df <- readr:: read_csv("Data_Upload/COUNTRY.csv")
customer_address_df <- readr::read_csv("Data_Upload/CUSTOMER_ADDRESS.csv")
order_df <- readr:: read_csv("Data_Upload/ORDERS.csv")
order_detail_df <- readr:: read_csv("Data_Upload/ORDER_DETAIL.csv")
order_status_df <- readr:: read_csv("Data_Upload/ORDER_STATUS.csv")
payment_method_df <- readr::read_csv("Data_Upload/PAYMENT_METHOD.csv")
shipping_df <- readr:: read_csv("Data_Upload/SHIPPING.csv")
supplier_df <- readr :: read_csv("Data_Upload/SUPPLIER.csv")
supplier_add_df <- readr :: read_csv("Data_Upload/SUPPLIER_ADDRESS.csv")
customer_df <- readr :: read_csv("Data_Upload/CUSTOMER.csv")
advert_df <- readr :: read_csv("Data_Upload/ADS.csv")

# Generate Data for SHIPPING
# Generate Dispatch date
set.seed(123)
#unique_order_ids <- unique(order_df$ORDER_ID)
days_to_add <- sample(0:7, length(order_detail_df$ORDER_ID), replace = TRUE)
days_to_add_df <- data.frame(order_id = order_detail_df$ORDER_ID, days_to_add = days_to_add)
shipping_df$DISPATCH_DATE <- as.Date(order_detail_df$PURCHASE_DATE[match(shipping_df$ORDER_ID, order_detail_df$ORDER_ID)], format = "%m/%d/%Y") + days_to_add_df$days_to_add[match(shipping_df$ORDER_ID, days_to_add_df$order_id)]

# Generate Delivery date
days_to_add <- sample(3:10, length(order_detail_df$ORDER_ID), replace = TRUE)
days_to_add_df <- data.frame(order_id = order_detail_df$ORDER_ID, days_to_add = days_to_add)
shipping_df$DELIVERY_DATE <- as.Date(shipping_df$DISPATCH_DATE,format = "%m/%d/%Y") + days_to_add_df$days_to_add[match(shipping_df$ORDER_ID, days_to_add_df$order_id)]

# Remove orders without shipping information
order_ids_status_2 <- order_detail_df$ORDER_ID[order_detail_df$ORDER_STATUS_ID == 2]
shipping_df$DISPATCH_DATE[shipping_df$ORDER_ID %in% order_ids_status_2] <- NA
shipping_df$DELIVERY_DATE[shipping_df$ORDER_ID %in% order_ids_status_2] <- NA

order_ids_status_4 <- order_detail_df$ORDER_ID[order_detail_df$ORDER_STATUS_ID == 4]
shipping_df$DELIVERY_DATE[shipping_df$ORDER_ID %in% order_ids_status_4] <- NA

order_ids_delete <- order_detail_df$ORDER_ID[order_detail_df$ORDER_STATUS_ID == 1 | order_detail_df$ORDER_STATUS_ID == 3]
shipping_df <- shipping_df[!shipping_df$ORDER_ID %in% order_ids_delete, ]

# Generate REVIEW_RATING
set.seed(123)
order_df <- order_df %>%
  mutate(REVIEW_RATING = sample(1:5, nrow(order_df), replace = TRUE))
order_no_review <- order_detail_df$ORDER_ID[order_detail_df$ORDER_STATUS_ID %in% c(1,2,3,4,6)]
order_df$REVIEW_RATING[order_df$ORDER_ID %in% order_no_review] <- NA



# Data Quality Check
order_detail_df$PURCHASE_DATE <- as.Date(order_detail_df$PURCHASE_DATE,format = "%m/%d/%Y")
advert_df$ADS_START_DATE <- as.Date(advert_df$ADS_START_DATE,format = "%m/%d/%Y")
advert_df$ADS_END_DATE <- as.Date(advert_df$ADS_END_DATE,format = "%m/%d/%Y")
customer_df$CUSTOMER_DOB <- as.Date(customer_df$CUSTOMER_DOB,format = "%m/%d/%Y")

shipping_df$DISPATCH_DATE <- format(shipping_df$DISPATCH_DATE,"%Y-%m-%d")
shipping_df$DELIVERY_DATE <- format(shipping_df$DELIVERY_DATE,"%Y-%m-%d")
order_detail_df$PURCHASE_DATE <- format(order_detail_df$PURCHASE_DATE,"%Y-%m-%d")
advert_df$ADS_START_DATE <- format(advert_df$ADS_START_DATE,"%Y-%m-%d")
advert_df$ADS_END_DATE <- format(advert_df$ADS_END_DATE,"%Y-%m-%d")
customer_df$CUSTOMER_DOB <- format(customer_df$CUSTOMER_DOB,"%Y-%m-%d")

convert_to_integer <- function(df) {
  cols <- grep("_ID$", names(df), value = TRUE)
  df[cols] <- lapply(df[cols], as.integer)
  return(df)
}

customer_df <- convert_to_integer(customer_df)
product_df <- convert_to_integer(product_df)
advert_df <- convert_to_integer(advert_df)
shipping_df <- convert_to_integer(shipping_df)
order_df <- convert_to_integer(order_df)
order_status_df <- convert_to_integer(order_status_df)
payment_method_df <- convert_to_integer(payment_method_df)
supplier_df <- convert_to_integer(supplier_df)
order_detail_df <- convert_to_integer(order_detail_df)

customer_address_df$CUSTOMER_ADDRESS_ID <- as.integer(customer_address_df$CUSTOMER_ADDRESS_ID)
supplier_add_df$SUPPLIER_ADDRESS_ID <- as.integer(supplier_add_df$SUPPLIER_ADDRESS_ID)

category_df$PRODUCT_CATEGORY_ID <- as.integer(sub("^0+", "", category_df$PRODUCT_CATEGORY_ID))
category_df$PARENT_CATEGORY_ID <- as.integer(sub("^0+", "", category_df$PARENT_CATEGORY_ID))

order_df$ORDER_ITEM_QTY <- as.integer(order_df$ORDER_ITEM_QTY)

product_df$PRODUCT_PRICE <- as.integer(product_df$PRODUCT_PRICE)
product_df$PRODUCT_QTY_AVAILABLE <- as.integer(product_df$PRODUCT_QTY_AVAILABLE)

customer_address_df$CUSTOMER_POSTCODE <- as.integer(customer_address_df$CUSTOMER_POSTCODE)
supplier_add_df$SUPPLIER_POSTCODE <- as.integer(supplier_add_df$SUPPLIER_POSTCODE)
customer_address_df$CUSTOMER_ADDRESS_NUMBER <- as.integer(customer_address_df$CUSTOMER_ADDRESS_NUMBER)
all_files <- list.files("Data_Upload/")
file_to_exclude <- "README.md"
all_files <- setdiff(all_files, file_to_exclude)

# Check number of columns and observations
for (variable in all_files) {
  this_filepath <- paste0("Data_Upload/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  number_of_rows <- nrow(this_file_contents)
  number_of_columns <- ncol(this_file_contents)
  
  print(paste0("The file: ",variable,
               " has: ",
               format(number_of_rows,big.mark = ","),
               " rows and ",
               number_of_columns," columns"))
}


## check unique constraint for primary key
for (variable in all_files){
  if (variable != "ORDERS.csv"){
    this_filepath <- paste0("Data_Upload/",variable)
    this_file_contents <- readr::read_csv(this_filepath)
    number_of_rows <- nrow(this_file_contents)
    print(paste0("Checking for: ",variable))
    print(paste0(" is ",nrow(unique(this_file_contents[,1]))==number_of_rows))
    next
  }else{
    this_filepath <- paste0("Data_Upload/",variable)
    this_file_contents <- readr::read_csv(this_filepath)
    number_of_rows <- nrow(this_file_contents)
    print(paste0("Checking for: ",variable))
    print(paste0(" is ",nrow(unique(this_file_contents[,c("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID")]))==number_of_rows))
  }
}


## checking the head of the data
df_list <- list(customer_df,advert_df, country_df, customer_address_df, category_df, 
                order_df, order_status_df, order_detail_df, 
                product_df, supplier_df, supplier_add_df, 
                payment_method_df, shipping_df)
names(df_list) <- c("customer_df","advert_df", "country_df", "customer_address_df", "category_df", 
                    "order_df", "order_status_df", "order_detail_df", 
                    "product_df", "supplier_df", "supplier_add_df", 
                    "payment_method_df", "shipping_df")

analyze_df <- function(df) {
  print(head(df))
}
results <- lapply(df_list, analyze_df)


## checking the structure of the data
analyze_df <- function(df) {
  print(str(df))
}
results <- lapply(df_list, analyze_df)

## checking the column names
analyze_df <- function(df) {
  print(colnames(df))
}
results <- lapply(df_list, analyze_df)

## Data Quality Checks 
### Pre-processing Quality Check
##checking for missing values
analyze_df <- function(df) {
  missing_values <- colSums(is.na(df))
  print("Missing values: ")
  print(missing_values)
}
results <- lapply(df_list, analyze_df)


## check for non-numeric values in numeric cols
analyze_df <- function(df) {
  non_numeric_values <- sapply(df, function(x) {
    sum(!is.na(as.numeric(x)))
  })
  print(non_numeric_values)
}
results <- lapply(df_list, analyze_df)


## check for unique values
df_list <- list(customer_df,advert_df, country_df, customer_address_df, category_df, 
                order_df, order_status_df, order_detail_df, 
                product_df, supplier_df, supplier_add_df, 
                payment_method_df, shipping_df)
names(df_list) <- all_files
for (df_name in names(df_list)) {
  df <- df_list[[df_name]]
  unique_values <- sapply(df, function(x) length(unique(x)))
  print(paste("Unique values in categorical columns of", df_name, ":"))
  print(unique_values)
}

# Write data to database
RSQLite::dbWriteTable(my_connection,"PRODUCT",product_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"CUSTOMER",customer_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"ORDERS",order_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"PAYMENT_METHOD",payment_method_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"SHIPPING",shipping_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"SUPPLIER",supplier_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"SUPPLIER_ADDRESS",supplier_add_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"CUSTOMER_ADDRESS",customer_address_df,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_connection,"ORDER_STATUS",order_status_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"COUNTRY",country_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"PRODUCT_CATEGORY",category_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"ADS",advert_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"ORDER_DETAIL",order_detail_df,overwrite=FALSE,append=TRUE) 


## Schema Validation
expected_PAYMENT_METHOD <- c("PAYMENT_METHOD_ID", "PAYMENT_METHOD_NAME")

expected_CUSTOMER <- c("CUSTOMER_ID", "CUSTOMER_EMAIL", "CUSTOMER_PHONE_NUMBER", "CUSTOMER_FIRST_NAME", "CUSTOMER_MIDDLE_NAME",  "CUSTOMER_LAST_NAME","CUSTOMER_DOB", "CUSTOMER_GENDER",  "CUSTOMER_ADDRESS_ID")

expected_ADS <- c("AD_ID", "PRODUCT_ID", "ADS_DESCRIPTION", "DISCOUNT_RATE", "ADS_START_DATE",  "ADS_END_DATE")

expected_PRODUCT_CATEGORY <- c("PRODUCT_CATEGORY_ID", "PARENT_CATEGORY_ID", "CATEGORY_NAME")

expected_COUNTRY <- c("COUNTRY_ID", "COUNTRY_NAME")

expected_CUSTOMER_ADDRESS <- c("CUSTOMER_ADDRESS_ID", "CUSTOMER_ADDRESS_NUMBER", "CUSTOMER_STREET", "CUSTOMER_POSTCODE", "CUSTOMER_CITY", "COUNTRY_ID")

expected_ORDER_DETAIL <- c("ORDER_ID", "ORDER_STATUS_ID", "PURCHASE_DATE", "PAYMENT_METHOD_ID")

expected_ORDER_STATUS <- c("ORDER_STATUS_ID", "ORDER_STATUS_NAME")

expected_ORDERS <- c("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID", "ORDER_ITEM_QTY","REVIEW_RATING")

expected_PRODUCT <- c("PRODUCT_ID", "PRODUCT_CATEGORY_ID", "SUPPLIER_ID", "PRODUCT_NAME", "PRODUCT_DESCRIPTION", "PRODUCT_PRICE", "PRODUCT_QTY_AVAILABLE")

expected_SHIPPING <- c("SHIPPING_ID", "ORDER_ID", "SHIPPING_NAME", "DISPATCH_DATE", "DELIVERY_DATE")

expected_SUPPLIER <- c("SUPPLIER_ID", "SUPPLIER_FIRST_NAME", "SUPPLIER_MIDDLE_NAME", "SUPPLIER_LAST_NAME", "SUPPLIER_ADDRESS_ID", "SUPPLIER_EMAIL", "SUPPLIER_PHONE", "SUPPLIER_AGREEMENT")

expected_SUPPLIER_ADDRESS <- c("SUPPLIER_ADDRESS_ID", "SUPPLIER_POSTCODE", "SUPPLIER_CITY", "COUNTRY_ID")


for (variable in all_files){
  table_name <-  gsub(".csv", "", variable)
  if (table_name == "CUSTOMER"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_CUSTOMER, actual_schema)) {
      print("CUSTOMER Schema validated sucessfully")
    }else{
      print("CUSTOMER Schema validation failed")
    }
  }
  if (table_name == "CUSTOMER_ADDRESS"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_CUSTOMER_ADDRESS, actual_schema)) {
      print("CUSTOMER_ADDRESS Schema validated sucessfully")
    }else{
      print("CUSTOMER_ADDRESS Schema validation failed")
    }
  }
  if (table_name == "ADS"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_ADS, actual_schema)) {
      print("ADS Schema validated sucessfully")
    }else{
      print("ADS Schema validation failed")
    }
  }
  if (table_name == "PRODUCT_CATEGORY"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_CATEGORY, actual_schema)) {
      print("PRODUCT_CATEGORY Schema validated sucessfully")
    }else{
      print("PRODUCT_CATEGORY Schema validation failed")
    }
  }
  if (table_name == "COUNTRY"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_COUNTRY, actual_schema)) {
      print("COUNTRY Schema validated sucessfully")
    }else{
      print("COUNTRY Schema validation failed")
    }
  }
  if (table_name == "ORDER_DETAIL"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_ORDER_DETAIL, actual_schema)) {
      print("ORDER_DETAIL Schema validated sucessfully")
    }else{
      print("ORDER_DETAIL Schema validation failed")
    }
  }
  if (table_name == "ORDER_STATUS"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_ORDER_STATUS, actual_schema)) {
      print("ORDER_STATUS Schema validated sucessfully")
    }else{
      print("ORDER_STATUS Schema validation failed")
    }
  }
  if (table_name == "ORDERS"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_ORDERS, actual_schema)) {
      print("ORDERS Schema validated sucessfully")
    }else{
      print("ORDERS Schema validation failed")
    }
  }
  if (table_name == "PAYMENT_METHOD"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_PAYMENT_METHOD, actual_schema)) {
      print("PAYMENT_METHOD Schema validated sucessfully")
    }else{
      print("PAYMENT_METHOD Schema validation failed")
    }
  }
  if (table_name == "PRODUCT"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_PRODUCT, actual_schema)) {
      print("PRODUCT Schema validated sucessfully")
    }else{
      print("PRODUCT Schema validation failed")
    }
  }
  if (table_name == "SHIPPING"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_SHIPPING, actual_schema)) {
      print("SHIPPING Schema validated sucessfully")
    }else{
      print("SHIPPING Schema validation failed")
    }
  }
  if (table_name == "SUPPLIER_ADDRESS"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_SUPPLIER_ADDRESS, actual_schema)) {
      print("SUPPLIER_ADDRESS Schema validated sucessfully")
    }else{
      print("SUPPLIER_ADDRESS Schema validation failed")
    }
  }
  if (table_name == "SUPPLIER"){
    actual_schema <- dbListFields(my_connection, table_name)
    if(identical(expected_SUPPLIER, actual_schema)) {
      print("SUPPLIER Schema validated sucessfully")
    }else{
      print("SUPPLIER Schema validation failed")
    }
  }
}
