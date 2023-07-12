install.packages("tidyverse", repos = "https://cran.rstudio.com/")
install.packages("sparklyr", repos="https://cran.rstudio.com")

library(sparklyr)
library(dplyr)
library(tidyr)
library(ggplot2)
library(magrittr)
library(knitr)

# Install
spark_install()

# Connect
sc <- sparklyr::spark_connect(master = "local")

datasetPath <- "/data/dataset.csv"
df <- spark_read_csv(sc, name = "neo_data", path = datasetPath, header = TRUE, infer_schema = TRUE)

# Filter data
df <- df %>%
  filter(!(is.na(H) || is.na(epoch) || is.na(diameter) || is.na(diameter_sigma))) %>%
  mutate(albedo = ifelse(is.na(albedo), 0, albedo))

# Check how much is lost
count(df)

# Create a new column that categorizes asteroids into different brightness categories based on their absolute magnitude (H)
df <- df %>%
  mutate(brightness = case_when(
    albedo < 0.25 ~ "Can't be seen",
    albedo >= 0.25 ~ "Moderately Bright",
    TRUE ~ "Can't be seen"
  ))

# Create a new column 
df <- df %>%
  mutate(can_be_seen = case_when(
    albedo < 0.25 ~ 0,
    albedo >= 0.25 ~ 1,
    TRUE ~ 0
  ))

# Create a new column that categorizes asteroids into different size categories based on their diameter (km)
df <- df %>%
  mutate(size_category = case_when(
    diameter < 1 ~ "Small",
    diameter >= 1 & diameter <= 10 ~ "Medium",
    diameter > 10 ~ "Large",
    TRUE ~ "Unknown"
    ))

# H devided by 5 (needed for formula)
df <- df %>%
  mutate(H_divd = H / 5)

# Sanity columns
colnames(df)

# Select most relevant data for further analising
df <- df %>% select(albedo, H, H_divd, diameter, diameter_sigma, epoch, can_be_seen, brightness, size_category)

# Just in case
df <- na.omit(df)

# Show selected data
kable(head(df, n = 10L),
      col.names = colnames(df),
      caption = "Table view of filtered data",
      format = "html",
      align = "c"
)

# Data split
df_split <- sdf_random_split(df, training = 0.7, test = 0.3)

# Check the split out
df_split

# Formula
formula <- can_be_seen ~ H + H_divd + diameter + albedo

# Logistic regression with different max iterations
max_iterations <- 5
# Create empty data frame
result_data <- data.frame(
  i = 1:max_iterations, 
  wp = numeric(max_iterations), 
  wr = numeric(max_iterations), 
  a = numeric(max_iterations),
  roc = numeric(max_iterations)
  )

for (i in 1:max_iterations) {
  # Traing the model
  model <- ml_logistic_regression(df_split$training, formula, max_iter = i, family = "binomial")
  
  # Evaluate the model
  result <- ml_evaluate(model, df_split$test)
  
  # Save results
  result_data$wp[i] <- result$weighted_precision()
  result_data$wr[i] <- result$weighted_recall()
  result_data$a[i] <- result$accuracy()
}

# Sanity
result_data

# Plotting Precision
plot1 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$wp) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "Precision", title = "a) Precision dependency on number of iterations")

# Plotting Recall
plot2 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$wr) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "Recall", title = "a) Recall dependency on number of iterations")

# Plotting Accuracy
plot3 <- result_data %>%
  ggplot(aes(i, wp, color = wp)) +
  geom_line(linewidth = 2) +
  scale_x_continuous(breaks = 1:max_iterations) +
  scale_y_continuous(breaks = result_data$a) +
  scale_color_gradient(low = "#FF2266", high = "#6622FF") +
  theme(text = element_text(size = 16)) +
  labs(x = "Num of iterations", y = "Accuracy", title = "a) Accuracy dependency on number of iterations")

# Display
plot1
plot2
plot3


# End the session
#spark_disconnect(sc)
