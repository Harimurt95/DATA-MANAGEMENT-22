library(readr)
library(RSQLite)
library(dplyr)

# Build connections
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "Database/sample.db")

# Read in csv file to dataframe
category_df <- readr::read_csv("Data_Upload/CATEGORY.csv")
product_df <- readr :: read_csv("Data_Upload/PRODUCT.csv")
country_df <- readr:: read_csv("Data_Upload/COUNTRY.csv")
customer_address_df <- readr::read_csv("Data_Upload/CUSTOMER_ADDRESS.csv")
order_df <- readr:: read_csv("Data_Upload/ORDER.csv")
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
unique_order_ids <- unique(order_df$ORDER_ID)
days_to_add <- sample(0:7, length(unique_order_ids), replace = TRUE)
days_to_add_df <- data.frame(order_id = unique_order_ids, days_to_add = days_to_add)
shipping_df$DISPATCH_DATE <- as.Date(order_df$PURCHASE_DATE[match(shipping_df$ORDER_ID, order_df$ORDER_ID)], format = "%m/%d/%Y") + days_to_add_df$days_to_add[match(shipping_df$ORDER_ID, days_to_add_df$order_id)]

# Generate Delivery date
days_to_add <- sample(3:10, length(unique_order_ids), replace = TRUE)
days_to_add_df <- data.frame(order_id = unique_order_ids, days_to_add = days_to_add)
shipping_df$DELIVERY_DATE <- as.Date(shipping_df$DISPATCH_DATE,format = "%m/%d/%Y") + days_to_add_df$days_to_add[match(shipping_df$ORDER_ID, days_to_add_df$order_id)]

# Remove orders without shipping information
order_ids_status_2 <- order_df$ORDER_ID[order_df$ORDER_STATUS_ID == 2]
shipping_df$DISPATCH_DATE[shipping_df$ORDER_ID %in% order_ids_status_2] <- NA
shipping_df$DELIVERY_DATE[shipping_df$ORDER_ID %in% order_ids_status_2] <- NA

order_ids_status_4 <- order_df$ORDER_ID[order_df$ORDER_STATUS_ID == 4]
shipping_df$DELIVERY_DATE[shipping_df$ORDER_ID %in% order_ids_status_4] <- NA

order_ids_delete <- order_df$ORDER_ID[order_df$ORDER_STATUS_ID == 1 | order_df$ORDER_STATUS_ID == 3]
shipping_df <- shipping_df[!shipping_df$ORDER_ID %in% order_ids_delete, ]

# Generate REVIEW_RATING
set.seed(123)
order_df <- order_df %>%
  mutate(REVIEW_RATING = sample(1:5, nrow(order_df), replace = TRUE))
order_no_review <- order_df$ORDER_ID[order_df$ORDER_STATUS_ID %in% c(1,2,3,4,6)]
order_df$REVIEW_RATING[order_df$ORDER_ID %in% order_no_review] <- NA



# Data Quality Check
order_df$PURCHASE_DATE <- as.Date(order_df$PURCHASE_DATE,format = "%m/%d/%Y")
advert_df$ADS_START_DATE <- as.Date(advert_df$ADS_START_DATE,format = "%m/%d/%Y")
advert_df$ADS_END_DATE <- as.Date(advert_df$ADS_END_DATE,format = "%m/%d/%Y")

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
customer_address_df$CUSTOMER_ADDRESS_ID <- as.integer(customer_address_df$CUSTOMER_ADDRESS_ID)
supplier_add_df$SUPPLIER_ADDRESS_ID <- as.integer(supplier_add_df$SUPPLIER_ADDRESS_ID)
category_df$CATEGORY_ID <- as.integer(sub("^0+", "", category_df$CATEGORY_ID))

category_df$PARENT_CATEGORY_ID <- as.integer(sub("^0+", "", category_df$PARENT_CATEGORY_ID))

order_df$ORDER_ITEM_QTY <- as.integer(order_df$ORDER_ITEM_QTY)

product_df$PRODUCT_PRICE <- as.integer(product_df$PRODUCT_PRICE)
product_df$PRODUCT_QTY_AVAILABLE <- as.integer(product_df$PRODUCT_QTY_AVAILABLE)

all_files <- list.files("Data_Upload/")
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

for (variable in all_files) {
  this_filepath <- paste0("Data_Upload/",variable)
  this_file_contents <- readr::read_csv(this_filepath)
  number_of_rows <- nrow(this_file_contents)
  print(paste0("Checking for: ",variable))
  print(paste0(" is ",nrow(unique(this_file_contents[,1]))==number_of_rows))
}

# Write data to database
RSQLite::dbWriteTable(my_connection,"PRODUCT",product_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"CUSTOMER",customer_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"ORDER",order_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"PAYMENT_METHOD",payment_method_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"SHIPPING",shipping_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"SUPPLIER",supplier_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"SUPPLIER_ADDRESS",supplier_add_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"CUSTOMER_ADDRESS",customer_address_df,overwrite=FALSE,append=TRUE)
RSQLite::dbWriteTable(my_connection,"ORDER_STATUS",order_status_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"COUNTRY",country_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"CATEGORY",category_df,overwrite=FALSE,append=TRUE) 
RSQLite::dbWriteTable(my_connection,"ADVERTISE",advert_df,overwrite=FALSE,append=TRUE) 


