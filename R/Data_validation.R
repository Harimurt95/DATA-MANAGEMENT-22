library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Build connections
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "Database/sample.db")

data_upload_path <- "Data_Upload"

all_files <- list.files(path = data_upload_path, pattern = "_\\d+\\.csv$", full.names = TRUE, recursive = TRUE)

create_df_name <- function(file_path) {
  name <- tools::file_path_sans_ext(basename(file_path))
  name <- gsub("_\\d+$", "", name)
  name <- gsub("[^A-Za-z0-9]+", "_", name)
  df_name <- tolower(paste0(name, "_df"))
  return(df_name)
}

# Read to dataframe
for (file_path in all_files) {
  data_frame_name <- create_df_name(file_path)
  assign(data_frame_name, read_csv(file_path))
}

## unit testing - checking row and columns
for (file_path in all_files){
  this_file_contents <- readr :: read_csv(file_path)
  number_of_rows <- nrow(this_file_contents)
  number_of_cols <- ncol(this_file_contents)
  print(paste0("this file: ",file_path, " has: ",       
               format(number_of_rows, big.mark = ","),
               " rows and ", number_of_cols, " columns"))
}

orders_files <- list.files(path = "Data_Upload/ORDERS_dataset", 
                           pattern = "^ORDERS_\\d+\\.csv$",
                           full.names = TRUE)

# Check for unique constraint for primary key
for (file_path in all_files){
  if (file_path != orders_files){
    this_file_contents <- readr::read_csv(file_path)
    number_of_rows <- nrow(this_file_contents)
    print(paste0("Checking for: ",file_path))
    print(paste0(" is ",nrow(unique(this_file_contents[,1]))==number_of_rows))
    next
  }else{
    this_file_contents <- readr::read_csv(file_path)
    number_of_rows <- nrow(this_file_contents)
    print(paste0("Checking for: ",file_path))
    print(paste0(" is ",nrow(unique(this_file_contents[,c("ORDER_ID", "CUSTOMER_ID", "PRODUCT_ID")]))==number_of_rows))
  }
}

# Convert data type
convert_to_integer <- function(df) {
  cols <- grep("_ID$", names(df), value = TRUE)
  df[cols] <- lapply(df[cols], as.integer)
  return(df)
}

if (exists("shipping_df")) {
  try({
    shipping_df$DISPATCH_DATE <- as.Date(shipping_df$DISPATCH_DATE,format = "%m/%d/%Y")
    shipping_df$DELIVERY_DATE <- as.Date(shipping_df$DELIVERY_DATE,format = "%m/%d/%Y")
    shipping_df <- convert_to_integer(shipping_df)
  }, silent = TRUE)
}


if (exists("ads_df")) {
  try({
    ads_df$ADS_START_DATE <- as.Date(ads_df$ADS_START_DATE,format = "%m/%d/%Y")
    ads_df$ADS_END_DATE <- as.Date(ads_df$ADS_END_DATE,format = "%m/%d/%Y")
    ads_df <- convert_to_integer(ads_df)
  }, silent = TRUE)
}


if (exists("customer_df")) {
  try({
    customer_df$CUSTOMER_DOB <- as.Date(customer_df$CUSTOMER_DOB,format = "%m/%d/%Y")
    customer_df <- convert_to_integer(customer_df)
  }, silent = TRUE)
}

if (exists("order_detail_df")) {
  try({
    order_detail_df$PURCHASE_DATE <- as.Date(order_detail_df$PURCHASE_DATE,format = "%m/%d/%Y")
    order_detail_df <- convert_to_integer(order_detail_df)
  }, silent = TRUE)
}

if (exists("orders_df")) {
  try({
    orders_df <- convert_to_integer(orders_df)
    orders_df$ORDER_ITEM_QTY <- as.integer(orders_df$ORDER_ITEM_QTY)
    orders_df$REVIEW_RATING <- as.integer(orders_df$REVIEW_RATING)
  }, silent = TRUE)
}

if (exists("supplier_df")) {
  try({
    supplier_df <- convert_to_integer(supplier_df)
  }, silent = TRUE)
}

if (exists("product_df")) {
  try({
    product_df <- convert_to_integer(product_df)
    product_df$PRODUCT_PRICE <- as.integer(product_df$PRODUCT_PRICE)
    product_df$PRODUCT_QTY_AVAILABLE <- as.integer(product_df$PRODUCT_QTY_AVAILABLE)
  }, silent = TRUE)
}

if (exists("customer_address_df")) {
  try({
    customer_address_df$CUSTOMER_ADDRESS_ID <- as.integer(customer_address_df$CUSTOMER_ADDRESS_ID)
    customer_address_df$CUSTOMER_POSTCODE <- as.integer(customer_address_df$CUSTOMER_POSTCODE)
    customer_address_df$CUSTOMER_ADDRESS_NUMBER <- as.integer(customer_address_df$CUSTOMER_ADDRESS_NUMBER)
  }, silent = TRUE)
}

if (exists("supplier_address_df")) {
  try({
    supplier_address_df$SUPPLIER_ADDRESS_ID <- as.integer(supplier_address_df$SUPPLIER_ADDRESS_ID)
    supplier_address_df$SUPPLIER_POSTCODE <- as.integer(supplier_address_df$SUPPLIER_POSTCODE)
  }, silent = TRUE)
}

if (exists("payment_method_df")) {
  try({
    payment_method_df <- convert_to_integer(payment_method_df)
  }, silent = TRUE)
}

if (exists("order_status_df")) {
  try({
    order_status_df <- convert_to_integer(order_status_df)
  }, silent = TRUE)
}

if (exists("product_category_df")) {
  try({
    product_category_df$PRODUCT_CATEGORY_ID <- as.integer(sub("^0+", "", product_category_df$PRODUCT_CATEGORY_ID))
    product_category_df$PARENT_CATEGORY_ID <- as.integer(sub("^0+", "", product_category_df$PARENT_CATEGORY_ID))
  }, silent = TRUE)
}


# Check column names
datasets <- ls(pattern = "_df$")
for (data in datasets) {
  cat(data, "columns:", "\n")
  print(names(get(data)))
  cat("\n")
}


# Check missing values
for (data in datasets) {
  df <- get(data)  
  if (is.data.frame(df)) {  
    missing_values <- colSums(is.na(df)) 
    print(missing_values)
  }
}

# Check unique values
for (data in datasets) {
  df <- get(data) 
  unique_values <- sapply(df, function(x) if (is.vector(x)) length(unique(x)) else NA)
  print(paste("Unique values in columns of", data, ":"))
  print(unique_values)
}

# Check structure
for (data in datasets){
  df <- get(data)
  str(df)
}

### checking for duplicated rows
for (index in seq_along(datasets)){
  if (any(duplicated(datasets[[index]]))){
    print("Duplicate values found!")
  } else{
    print("Duplicates not found!")
  }
}

library(validate)

## Customer Data
### check email column in customers dataset
data <- customer_df$CUSTOMER_EMAIL
check_email_quality <- function(emails) {
  valid <- grepl("\\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}\\b", emails)
  return(valid)
}
email_quality <- check_email_quality(data)
print(email_quality)


### checking the phone number of customers in dataset
rules <- validator(nchar(gsub("-", "", customer_df$CUSTOMER_PHONE_NUMBER)) == 10)
out <- confront(customer_df, rules)
summary(out)

### checking the customer_first_name values
data <- customer_df$CUSTOMER_FIRST_NAME
check_name_quality <- function(names){
  valid <- grepl("\\b[A-Za-z]",names)
}
name_quality <- check_name_quality(data)
print(name_quality)

### checking the customer_middle_name values
data <- customer_df$CUSTOMER_MIDDLE_NAME
check_name_quality <- function(names){
  valid <- grepl("\\b[A-Za-z]",names)
}
name_quality <- check_name_quality(data)
print(name_quality)

### checking the customer_last_name values
data <- customer_df$CUSTOMER_LAST_NAME
check_name_quality <- function(names){
  valid <- grepl("\\b[A-Za-z]",names)
}
name_quality <- check_name_quality(data)
print(name_quality)

# Check start date and end date of advertisement
if (exists("ads_df")) {
  start_date <- ads_df$ADS_START_DATE
  end_date <- ads_df$ADS_END_DATE
  
  if (all(start_date < end_date)) {
    print("Start date is before end date")
  } else {
    print("Start date is after end date")
  }
} else {
  print("ads_df does not exist")
}

# Check dispatch date and delivery date
if (exists("shipping_df")) {
  dispatch_date <- shipping_df$DISPATCH_DATE
  delivery_date <- shipping_df$DELIVERY_DATE
  
  if (all(dispatch_date < delivery_date, na.rm = TRUE)) {
    print("All dispatch dates are before delivery dates")
  } else {
    print("Some dispatch dates are after delivery dates or there are NA values")
  }
} else {
  print("shipping_df does not exist")
}


all_tables <- c("ADS", "PRODUCT_CATEGORY", "COUNTRY", "CUSTOMER_ADDRESS", "CUSTOMER", 
                "ORDER_DETAIL", "ORDER_STATUS", "ORDERS", "PAYMENT_METHOD",
                "PRODUCT", "SHIPPING", "SUPPLIER_ADDRESS", "SUPPLIER")

data_frames <- setNames(lapply(all_tables, dbReadTable, conn = my_connection), all_tables)
list2env(data_frames, envir = .GlobalEnv)


### checking referential integrity for customer_df and order_df
if (((exists("customer_df") || (exists("CUSTOMER"))) && exists("orders_df"))) {
  p_key <- unique(c(customer_df$CUSTOMER_ID,CUSTOMER$CUSTOMER_ID))
  f_key <- orders_df$CUSTOMER_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
}

### checking referential integrity for product_df and orders_df
if (exists("orders_df") && (exists("product_df") || exists("PRODUCT"))) {
  if (exists("product_df")) {
    p_key <- unique(c(product_df$PRODUCT_ID,PRODUCT$PRODUCT_ID))
  } else if (exists("PRODUCT")) {
    p_key <- PRODUCT$PRODUCT_ID
  }
  f_key <- orders_df$PRODUCT_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("orders_df does not exist")
}


### checking referential integrity for order_details and order_df
if (exists("order_detail_df") && exists("orders_df")) {
  p_key <- order_detail_df$ORDER_ID
  f_key <- orders_df$ORDER_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
}

### checking referential integrity for ads_df and product_df
if (exists("ads_df") && (exists("product_df") || exists("PRODUCT"))) {
  if (exists("product_df")) {
    p_key <- unique(c(product_df$PRODUCT_ID,PRODUCT$PRODUCT_ID))
  } else if (exists("PRODUCT")) {
    p_key <- PRODUCT$PRODUCT_ID
  }
  f_key <- ads_df$PRODUCT_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("Referential Integrity not maintained")
  }
} else {
  print("ads_df does not exist")
}

### checking referential integrity for order_detail_df and order_status_df
if ((exists("order_detail_df")) && ((exists("ORDER_STATUS")) || (exists("order_status_df")))) {
  f_key <- order_detail_df$ORDER_STATUS_ID
  if (exists("order_status_df")) {
    p_key <- order_detail_df$ORDER_STATUS_ID
  } else p_key <- ORDER_STATUS$ORDER_STATUS_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
}


### checking referential integrity for category_df and product_df
if (((exists("PRODUCT_CATEGORY")) || exists("category_df")) && ((exists("product_df") || exists("PRODUCT")))) {
  if (exists("product_df")) {
    f_key <- unique(c(product_df$PRODUCT_CATEGORY_ID,PRODUCT$PRODUCT_CATEGORY_ID))
  } else if (exists("PRODUCT")) {
    f_key <- PRODUCT$PRODUCT_CATEGORY_ID
  }
  if (exists("category_df")){
    p_key <- category_df$PRODUCT_CATEGORY_ID
  } else p_key <- PRODUCT_CATEGORY$PRODUCT_CATEGORY_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("something wrong")
}

### checking the referential integrity for supplier_df and product_df
if (exists("SUPPLIER") || exists("supplier_df") && (exists("product_df"))) {
  if (exists("product_df")) {
    f_key <- product_df$SUPPLIER_ID
  } 
  if (exists("supplier_df")){
    p_key <- unique(c(supplier_df$SUPPLIER_ID,SUPPLIER$SUPPLIER_ID))
  } else if (exists("SUPPLIER")) {
    p_key <- SUPPLIER$SUPPLIER_ID
  }
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("supplier_df does not exist")
}


### checking referential integrity for orders_df and shipping_df
if (((exists("order_detail_df") || (exists("ORDER_DETAIL"))) && (exists("shipping_df")))) {
  p_key <- unique(c(order_detail_df$ORDER_ID,ORDER_DETAIL$ORDER_ID))
  f_key <- shipping_df$ORDER_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("Referential Integrity not maintained")
  }
} else {
  print("Either orders_df or shipping_df does not exist")
}

### checking referential integrity for country_df and supplier_address_df
if (((exists("COUNTRY")) || (exists("country_df")) && (exists("supplier_address_df")))) {
  f_key <- supplier_address_df$COUNTRY_ID
  if (exists("country_df")) {
    p_key <- country_df$COUNTRY_ID
  } else p_key <- COUNTRY$COUNTRY_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("supplier_address_df does not exist")
}


### checking referential integrity for country_df and customer_address_df
if (((exists("COUNTRY")) || (exists("country_df")) && (exists("customer_address_df")))) {
  f_key <- customer_address_df$COUNTRY_ID
  if (exists("country_df")){
    p_key <- country_df$COUNTRY_ID
  } else p_key <- COUNTRY$COUNTRY_ID
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("supplier_address_df does not exist")
}

### checking referential integrity for customer_df and customer_address_df
if (exists("customer_df") || exists("customer_address_df") && (exists("CUSTOMER_ADDRESS"))) {
  if (exists("customer_df")) {
    f_key <- customer_df$CUSTOMER_ADDRESS_ID
  } 
  if (exists("customer_address_df")){
    p_key <- unique(c(customer_address_df$CUSTOMER_ADDRESS_ID,CUSTOMER_ADDRESS$CUSTOMER_ADDRESS_ID))
  } else if (exists("CUSTOMER_ADDRESS")) {
    f_key <- CUSTOMER_ADDRESS$CUSTOMER_ADDRESS_ID
  }
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("customer_df does not exist")
}

### checking referential integrity for suppllier_df and supplier_address_df
if (exists("supplier_df") || exists("supplier_address_df") && (exists("SUPPLIER_ADDRESS"))) {
  if (exists("supplier_df")) {
    f_key <- supplier_df$SUPPLIER_ADDRESS_ID
  } 
  if (exists("supplier_address_df")){
    p_key <- unique(c(supplier_address_df$SUPPLIER_ADDRESS_ID,SUPPLIER_ADDRESS$SUPPLIER_ADDRESS_ID))
  } else if (exists("SUPPLIER_ADDRESS")) {
    f_key <- SUPPLIER_ADDRESS$SUPPLIER_ADDRESS_ID
  }
  if (all(f_key %in% p_key)) {
    print("Referential Integrity maintained")
  } else {
    print("not maintained")
  }
} else {
  print("customer_df does not exist")
}


if (exists("shipping_df")) {
  try({
    shipping_df$DISPATCH_DATE <- format(shipping_df$DISPATCH_DATE,"%Y-%m-%d")
    shipping_df$DELIVERY_DATE <- format(shipping_df$DELIVERY_DATE,"%Y-%m-%d")
  }, silent = TRUE)
}


if (exists("ads_df")) {
  try({
    ads_df$ADS_START_DATE <- format(ads_df$ADS_START_DATE,"%Y-%m-%d")
    ads_df$ADS_END_DATE <- format(ads_df$ADS_END_DATE,"%Y-%m-%d")
  }, silent = TRUE)
}


if (exists("customer_df")) {
  try({
    customer_df$CUSTOMER_DOB <- format(customer_df$CUSTOMER_DOB,"%Y-%m-%d")
  }, silent = TRUE)
}

if (exists("order_detail_df")) {
  try({
    order_detail_df$PURCHASE_DATE <- format(order_detail_df$PURCHASE_DATE,"%Y-%m-%d")
  }, silent = TRUE)
}

if (exists("customer_df")) {
  try({
    existence_check <- customer_df[[1]] %in% CUSTOMER[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("customer_address_df")) {
  try({
    existence_check <- customer_address_df[[1]] %in% CUSTOMER_ADDRESS[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("supplier_address_df")) {
  try({
    existence_check <- supplier_address_df[[1]] %in% SUPPLIER_ADDRESS[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("order_detail_df")) {
  try({
    existence_check <- order_detail_df[[1]] %in% ORDER_DETAIL[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("product_df")) {
  try({
    existence_check <- product_df[[1]] %in% PRODUCT[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("supplier_df")) {
  try({
    existence_check <- supplier_df[[1]] %in% SUPPLIER[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("shipping_df")) {
  try({
    existence_check <- shipping_df[[1]] %in% SHIPPING[[1]]
    print(existence_check)
  }, silent = TRUE)
}

if (exists("ads_df")) {
  try({
    existence_check <- ads_df[[1]] %in% ADS[[1]]
    print(existence_check)
  }, silent = TRUE)
}









