library(readr)
library(RSQLite)
library(dplyr)
library(DBI)
library(openxlsx)

# Build connections
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "Database/sample.db")

all_tables <- c("ADS", "PRODUCT_CATEGORY", "COUNTRY", "CUSTOMER_ADDRESS", "CUSTOMER", 
                "ORDER_DETAIL", "ORDER_STATUS", "ORDERS", "PAYMENT_METHOD",
                "PRODUCT", "SHIPPING", "SUPPLIER_ADDRESS", "SUPPLIER")

data_frames <- setNames(lapply(all_tables, dbReadTable, conn = my_connection), all_tables)
list2env(data_frames, envir = .GlobalEnv)

# Analysis for order status
order_status_info <- ORDER_DETAIL %>%
  group_by(ORDER_STATUS_ID) %>%
  summarise(Frequency = n()) %>%
  mutate(Order_Status = case_when(
    ORDER_STATUS_ID == 1 ~ "Pending",
    ORDER_STATUS_ID == 2 ~ "Accepted",
    ORDER_STATUS_ID == 3 ~ "Rejected",
    ORDER_STATUS_ID == 4 ~ "Delievring",
    ORDER_STATUS_ID == 5 ~ "Closed",
    ORDER_STATUS_ID == 6 ~ "Returned",
    TRUE ~ as.character(ORDER_STATUS_ID)))

excel_filename <- paste0("Excel_results/order_status_info_", this_filename_date, "_", this_filename_time, ".xlsx")
write.xlsx(order_status_info, file = excel_filename)
