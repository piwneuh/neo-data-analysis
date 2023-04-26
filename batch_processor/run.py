from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder \
        .appName("Batch processing") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0") \
        .config("spark.executor.extraClassPath", "/opt/workspace/postgresql-42.4.0.jar") \
        .master("spark://spark-master:7077") \
        .getOrCreate()
    )

    source_path = f'hdfs://namenode:9000/neo/dataset.csv'
    local_path = "../data/dataset.csv"

    print('reading source')
    df = spark.read.csv(
        source_path, inferSchema=True, header=True)

    # Show the resulting DataFrame
    df.show()

if __name__ == '__main__':
    main()
