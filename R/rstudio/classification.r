install.packages("tidyverse", repos = "https://cran.rstudio.com/")
install.packages("gridExtra", repos = "https://cran.rstudio.com/")
install.packages("kableExtra", repos = "https://cran.rstudio.com/")
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
  filter(!(is.na(H) || is.na(epoch) || is.na(diameter) || is.na(diameter_sigma)))


df <- df %>%
  mutate(albedo = ifelse(is.na(albedo), 0, albedo))

# Check how much is lost
count(df)

# Create a new column that categorizes asteroids into different brightness categories based on their absolute magnitude (H)
df <- df %>%
  mutate(brightness = case_when(
    H < 15 ~ "Very Bright",
    H >= 15 & H < 20 ~ "Moderately Bright",
    TRUE ~ "Dim"
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
df <- df %>% select(albedo, H, H_divd, diameter, diameter_sigma, epoch, brightness, size_category)

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
formula <- albedo ~ ((1329 * 10 (-H_divd))/diameter)^2

# Regression
model1 <- ml_logistic_regression(df_split$training, formula, max_iter = 5, family = "binomial")

# Traing the model
m1 <- ml_fit(model1, df_split$training)

# Evaluate the model
result <- ml_evaluate(model1, data_split$test)

# End the session
#spark_disconnect(sc)
