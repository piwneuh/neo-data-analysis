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

    print('reading source')
    df = spark.read.csv(
        source_path, inferSchema=True, header=True)
    # TODO: Format the dataset and write it to a postgres table

    # Show the resulting DataFrame
    df.show()

    # Save to posgres
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/neo_db") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "neo") \
        .option("user", "postgres").option("password", "admin").save()
    
    # Find the percentage of Small Bodies that are Near Earth Objects
    total_rows = df.count()
    neo_true_count = df.filter(df.neo == 'Y').count()
    neo_true_percent = (neo_true_count / total_rows) * 100

    print("Percentage of Small bodies that are NEOs:", neo_true_percent)
    print("Percentage of Small bodies that are not NEOs", 100 - neo_true_percent)

    # TODO: Write queries to answer interesting questions about the dataset

if __name__ == '__main__':
    main()
