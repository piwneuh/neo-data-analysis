from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

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

    # Create a new column that categorizes asteroids into different brightness categories based on their absolute magnitude (H)
    df = df.withColumn("abs_mag_category", when(col("H") < 15, "Very Bright")
        .when((col("H") >= 15) & (col("H") < 20), "Moderately Bright")
        .otherwise("Dim"))

    # Create a new column that categorizes asteroids into different orbit class categories based on their orbit class
    df = df.withColumn("orbit_class_category", 
                   when(df.orbit_id.startswith("M"), "Main Belt")
                   .when(df.orbit_id.startswith("O"), "Outer Solar System")
                   .when(df.orbit_id.startswith("I"), "Inner Solar System")
                   .otherwise("Unknown"))

    # Create a new column that categorizes asteroids into different size categories based on their diameter (km)
    df = df.withColumn("size_category", 
                   when(df.diameter < 1, "Small")
                   .when((df.diameter >= 1) & (df.diameter <= 10), "Medium")
                   .when(df.diameter > 10, "Large")
                   .otherwise("Unknown"))
    
    # Show the resulting DataFrame
    df.show()

    # Save to posgres
    df.write.format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/neo_db") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "neo") \
        .option("user", "postgres").option("password", "admin").save()
    
    #TODO: Write more queries to answer interesting questions about the dataset

    # Find the percentage of Small Bodies that are Near Earth Objects
    total_rows = df.count()
    neo_true_count = df.filter(df.neo == 'Y').count()
    neo_true_percent = (neo_true_count / total_rows) * 100

    print("Percentage of Small bodies that are NEOs:", neo_true_percent)
    print("Percentage of Small bodies that are not NEOs", 100 - neo_true_percent)

if __name__ == '__main__':
    main()
