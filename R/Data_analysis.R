library(ggplot2)
library(readr)
library(RSQLite)
library(dplyr)
library(DBI)

# Build connections
my_connection <- RSQLite::dbConnect(RSQLite::SQLite(), "Database/sample.db")

all_tables <- c("ADS", "PRODUCT_CATEGORY", "COUNTRY", "CUSTOMER_ADDRESS", "CUSTOMER", 
                "ORDER_DETAIL", "ORDER_STATUS", "ORDERS", "PAYMENT_METHOD",
                "PRODUCT", "SHIPPING", "SUPPLIER_ADDRESS", "SUPPLIER")

data_frames <- setNames(lapply(all_tables, dbReadTable, conn = my_connection), all_tables)
list2env(data_frames, envir = .GlobalEnv)

library(ggplot2)
average_reviews <- ORDERS %>%
  group_by(ORDER_ID) %>%
  summarize(avg_review_rating = mean(REVIEW_RATING, na.rm = TRUE))

status_orders <- ORDER_DETAIL %>%
  filter(ORDER_STATUS_ID == 5) %>%
  inner_join(average_reviews, by = "ORDER_ID")

merge_data <- SHIPPING %>%
  inner_join(status_orders, by = "ORDER_ID")

merge_data <- merge_data %>%
  mutate(
    DISPATCH_DATE = as.Date(DISPATCH_DATE),
    DELIVERY_DATE = as.Date(DELIVERY_DATE),
    waiting_days = as.numeric(DELIVERY_DATE - DISPATCH_DATE))

ggplot(merge_data, aes(x = waiting_days, y = avg_review_rating)) +
  geom_smooth(method="lm") +
  labs(x = "Waiting Days", y = "Review Score", title = "Review Score by Waiting Days") +
  theme_minimal()

this_filename_date <- as.character(Sys.Date())
# format the Sys.time() to show only hours and minutes 
this_filename_time <- as.character(format(Sys.time(), format = "%H_%M"))
ggsave(paste0("Figures/analysis_plot_",
              this_filename_date,"_",
              this_filename_time,".png"))