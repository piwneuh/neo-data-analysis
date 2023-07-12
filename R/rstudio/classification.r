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

# Sanity
glimpse(df)

# Columns
colnames(df)

# Filter data
df <- df %>%
  filter(!is.na(diameter_sigma))

# Exclude the "prefix" column
df <- select(df, -prefix)

# Columns sanity
colnames(df)

# Create a new column that categorizes asteroids into different brightness categories based on their absolute magnitude (H)
df <- df %>%
  mutate(abs_mag_category = case_when(
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

# Sanity columns
colnames(df)

# Show data
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

# Regression
model1 <- ml_logistic_regression(sc, label_col = "albedo", features_col = c("diameter", "H"))

# Traing the model
m1 <- ml_fit(model1, df_split$training)

# Evaluate the model
result <- ml_evaluate(model1, data_split$test)

# End the session
#spark_disconnect(sc)
