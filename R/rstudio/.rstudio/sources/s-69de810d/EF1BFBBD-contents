library(sparklyr)
library(dplyr)

# Install
spark_install()

# Connect
sc <- sparklyr::spark_connect(master = "local")

datasetPath <- "/data/dataset.csv"
df <- spark_read_csv(sc, name = "neo_data", path = datasetPath, header = TRUE, infer_schema = TRUE)

# Display data
glimpse(df)
head(df)

# Exploring data
colnames(df)
summary(df)

# Select specific columns
selected_df <- df %>%
  select(id, full_name, H, diameter)
selected_df

# Group by a column and compute aggregate functions
grouped_df <- df %>%
  group_by(class) %>%
  summarise(mean_H = mean(H), max_diameter = max(diameter))
grouped_df

# Collect the resulting data to the local R environment
local_df <- collect(selected_df)
local_df

spark_disconnect(sc)
