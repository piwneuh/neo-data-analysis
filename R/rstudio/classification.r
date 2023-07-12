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
results <- vector("list", max_iterations)

for (i in 1:max_iterations) {
  
  # Traing the model
  model <- ml_logistic_regression(df_split$training, formula, max_iter = i, family = "binomial")
  
  # Evaluate the model
  result <- ml_evaluate(model, df_split$test)
  
  # Save results
  results[[i]] <- result
}

results[[1]]$weighted_precision()

# End the session
#spark_disconnect(sc)
