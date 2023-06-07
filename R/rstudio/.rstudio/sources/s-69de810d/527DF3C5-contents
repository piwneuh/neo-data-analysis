library(sparklyr)
library(dplyr)

# Install
#spark_install()

# Connect
sc <- sparklyr::spark_connect(master = "local")

datasetPath <- "/data/dataset.csv"
df <- spark_read_csv(sc, name = "neo_data", path = datasetPath, header = TRUE, infer_schema = TRUE)

# Sanity
glimpse(df)

# End the session
spark_disconnect(sc)
