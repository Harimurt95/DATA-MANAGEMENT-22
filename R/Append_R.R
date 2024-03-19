# APPENDING DATA
library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Build connections
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "Database/sample.db")


### set a function to list all csv files in the assigned path
list_csv_files <- function(folder_path){
  files <- list.files(path = folder_path, pattern = "\\.csv$",
                      full.names = TRUE)
  return (files)
}

### create a folder and table name mapping for later use
folder_table_mapping <- list(
  "ADS_dataset" = "ADS",
  "PRODUCT_CATEGORY_dataset"="PRODUCT_CATEGORY",
  "PRODUCT_dataset"  = "PRODUCT",
  "COUNTRY_dataset" = "COUNTRY", 
  "CUSTOMER_dataset" = "CUSTOMER",
  "CUSTOMER_ADDRESS_dataset" = "CUSTOMER_ADDRESS",
  "ORDER_DETAIL_dataset" = "ORDER_DETAIL",
  "ORDERS_dataset" = "ORDERS",
  "ORDER_STATUS_dataset" = "ORDER_STATUS",
  "PAYMENT_METHOD_dataset"="PAYMENT_METHOD",
  "SHIPPING_dataset" = "SHIPPING",
  "SUPPLIER_dataset"="SUPPLIER",
  "SUPPLIER_ADDRESS_dataset"="SUPPLIER_ADDRESS"
)


column_types_mapping <- list(
  "PRODUCT_CATEGORY" = c("PRODUCT_CATEGORY_ID" = "int", "PARENT_CATEGORY_ID" = "int"),
  "CUSTOMER" = c("CUSTOMER_ID" = "int", "CUSTOMER_ADDRESS_ID" = "int"),
  "CUSTOMER_ADDRESS_ID" = c("CUSTOMER_ADDRESS_ID" = "int","CUSTOMER_POSTCODE"="int"),
  "SUPPLIER" = c("SUPPLIER_ID" = "int","SUPPLIER_ADDRESS_ID" = "int"),
  "SUPPLIER_ADDRESS" = c("SUPPLIER_ID"="int","SUPPLIER_POSTCODE"="int"),
  "PRODUCT" = c("PRODUCT_ID" = "int", "PRODUCT_CATEGORY_ID"="int","SUPPLIER_ID" = "int", 
                "PRODUCT_PRICE"="int", "PRODUCT_QTY_AVAILABLE" = "int"),
  "SHIPPING" = c("SHIPPING_ID" = "int","ORDER_ID"="int"),
  "ADS" = c("AD_ID"="int","PRODUCT_ID"="int"),
  "ORDERS" = c("ORDER_ID"="int","CUSTOMER_ID"="int","PRODUCT_ID"="int",
               "ORDER_ITEM_QTY"="int","REVIEW_RATING"="int"),
  "ORDER_DETAIL" = c("ORDER_ID"="int","ORDER_STATUS_ID"="int","PAYMENT_METHOD_ID"="int"),
  "ORDER_STATUS" = c("ORDER_STATUS_ID"="int"),
  "PAYMENT_METHOD" = c("PAYMENT_METHOD_ID"="int")
)


### making sure some columns are in the data type we want before writing data
convert_column_types <- function(data, column_types) {
  for (col_name in names(column_types)) {
    if (col_name %in% names(data)) {
      col_type <- column_types[[col_name]]
      if (col_type == "character") {
        data[[col_name]] <- as.character(data[[col_name]])
      } else if (col_type == "date") {
        date[[col_name]] <- as.Date(data[[col_name]], format = "%m/%d/%y")
        date[[col_name]] <- format(data[[col_name]],"%Y-%m-%d")
      }
    } }
  return(data)
}


### path to the main folder containing sub-folders
main_folder <- "Data_Upload"

pattern <- "_\\d+\\.csv$"

for (folder_name in names(folder_table_mapping)){
  folder_path <- file.path(main_folder, folder_name)
  if (dir.exists(folder_path)) {
    cat("Processing folder:", folder_name, "\n")
    # List CSV files in the sub-folder
    csv_files <- list_csv_files(folder_path)
    csv_files <- grep(pattern, csv_files, value = TRUE)
    # Get the corresponding table name from the mapping
    table_name <- folder_table_mapping[[folder_name]]
    # Append data from CSV files to the corresponding table
    for (csv_file in csv_files) {
      cat("Appending data from:", csv_file, "\n")
      tryCatch({
        # Read CSV file
        file_contents <- readr::read_csv(csv_file)
        # Convert column data types
        file_contents <- convert_column_types(file_contents,
                                              column_types_mapping[[table_name]])
        # Append data to the table in SQLite
        RSQLite::dbWriteTable(my_connection, table_name, file_contents,
                              append = TRUE, overwrite=FALSE)
        cat("Data appended to table:", table_name, "\n")
      }, error = function(e) {
        cat("Error appending data:", csv_file, "\n")
        cat("Error message:", e$message, "\n")
      }) }
  } else {
    cat("Folder does not exist:", folder_path, "\n")
  } }

tables <- RSQLite::dbListTables(my_connection)
print(tables)

