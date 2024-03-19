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

# Analysis for reveiw ratings
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
ggsave(paste0("Figures/ReveiwRating_plot_",
              this_filename_date,"_",
              this_filename_time,".png"))

# Analysis for product category
product_info <- PRODUCT %>%
  mutate(category_group = case_when(
    PRODUCT_CATEGORY_ID >= 11 & PRODUCT_CATEGORY_ID <= 15 ~ "Clothes",
    PRODUCT_CATEGORY_ID >= 21 & PRODUCT_CATEGORY_ID <= 29 ~ "Electronics",
    PRODUCT_CATEGORY_ID >= 31 & PRODUCT_CATEGORY_ID <= 39 ~ "Kitchenware",
    TRUE ~ as.character(PRODUCT_CATEGORY_ID) 
  ))

product_counts <- product_info %>%
  group_by(category_group) %>%
  summarise(count = n())

ggplot(product_counts, aes(x = category_group, y = count)) +
  geom_bar(stat = "identity") +
  theme_minimal() +
  labs(title = "Number of Products in Each Parent Category",
       x = "Category Group",
       y = "Number of Products")

ggsave(paste0("Figures/Category_plot_",
              this_filename_date,"_",
              this_filename_time,".png"))

# Analysis for payment method
payment_info <- ORDER_DETAIL %>%
  group_by(PAYMENT_METHOD_ID) %>%
  summarise(Frequency = n()) %>%
  mutate(Payment_Method = case_when(
    PAYMENT_METHOD_ID == 1 ~ "Credit Card",
    PAYMENT_METHOD_ID == 2 ~ "Debit Card",
    PAYMENT_METHOD_ID == 3 ~ "Cash on Delivery",
    TRUE ~ as.character(PAYMENT_METHOD_ID)))

ggplot(payment_info, aes(x=Payment_Method,y=Frequency)) +
  geom_bar(stat = "identity",width = 0.5) +
  theme_minimal() +
  labs(title = "Orders paid via different payment methods",
       x = "Payment method",
       y = "Number of Orders")

ggsave(paste0("Figures/payment_plot_",
              this_filename_date,"_",
              this_filename_time,".png"))