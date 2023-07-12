install.packages("tidyverse", repos = "https://cran.rstudio.com/")
install.packages("gridExtra", repos = "https://cran.rstudio.com/")
install.packages("kableExtra", repos = "https://cran.rsdtuio.com/")


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

# Create a new column that categorizes asteroids into different brightness categories based on their absolute magnitude (H)
df <- df %>%
  mutate(abs_mag_category = case_when(
    H < 15 ~ "Very Bright",
    H >= 15 & H < 20 ~ "Moderately Bright",
    TRUE ~ "Dim"
  ))

# Create a new column that categorizes asteroids into different orbit class categories based on their orbit class
df <- df %>%
  mutate(orbit_class_category = case_when(
    startsWith(orbit_id, "M") ~ "Main Belt",
    startsWith(orbit_id, "O") ~ "Outer Solar System",
    startsWith(orbit_id, "I") ~ "Inner Solar System",
    TRUE ~ "Unknown"
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

# End the session
#spark_disconnect(sc)