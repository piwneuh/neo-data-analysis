from pyspark.sql.functions import desc, avg, stddev, floor, first, last
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.window import Window

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

    #Find the top 10 largest asteroids by diameter
    window = Window.orderBy(desc("diameter"))

    top10 = df.select("id", "full_name", "diameter", "albedo") \
            .filter("diameter is not null") \
            .withColumn("avg_albedo", avg("albedo").over(window.rowsBetween(-9, 0))) \
            .withColumn("stddev_albedo", stddev("albedo").over(window.rowsBetween(-9, 0))) \
            .orderBy(desc("diameter")) \
            .limit(10)

    top10.show()
    
    top10.write.format("jdbc") \
        .option("url", "jdbc:postgresql://db:5432/neo_db") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "largest_asteroids") \
        .option("user", "postgres").option("password", "admin").save()
    
    #Find the average H magnitude of asteroids that have been flagged as potentially hazardous (PHA) and non-PHA, by decade of discovery:
    decade = floor(df.epoch / 10) * 10

    pha_avg_h = df.select("pha", "H", "epoch") \
                .filter("H is not null") \
                .groupBy("pha", decade) \
                .agg(avg("H").alias("avg_H")) \
                .orderBy("pha", decade)
    
    pha_avg_h.show()

    pha_avg_h.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/neo_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "pha_avg_h") \
            .option("user", "postgres").option("password", "admin").save()
    
    # Grouping NEOs by size, orbit class, and absolute magnitude category
    neo_group_set = df.filter(col('neo') == 'Y') \
            .groupBy('abs_mag_category', 'orbit_class_category', 'size_category') \
            .agg(avg('diameter').alias('avg_diameter'),
                avg('albedo').alias('avg_albedo'),
                avg('q').alias('avg_perihelion_distance'))
    
    neo_group_set.show()

    neo_group_set.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/neo_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "neo_group_set") \
            .option("user", "postgres").option("password", "admin").save()

    # Find large asteroids that are close to the Sun
    perh_dist = df.filter((df['diameter'] > 1000) & (df['q'] < 0.5)) \
           .groupBy('class') \
           .count()

    perh_dist.show()

    perh_dist.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/neo_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "perh_dist") \
            .option("user", "postgres").option("password", "admin").save()

    # Find hazardous asteroids
    hazardous_asteroids = df.filter(col('pha') == 'Y') \
                        .select('full_name', 'diameter', 'albedo', 'epoch', 'per', 'moid') \
                        .orderBy(col('moid'))

    hazardous_asteroids.show()

    hazardous_asteroids.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/neo_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "hazardous_asteroids") \
            .option("user", "postgres").option("password", "admin").save()

    close_hazardous_asteroids = df.filter(col('neo') == 'Y') \
           .selectExpr('abs(epoch_mjd - 58484.0) as time_to_closest_approach', 
                       'full_name', 'diameter', 'albedo') \
           .orderBy('time_to_closest_approach')

    close_hazardous_asteroids.show()

    close_hazardous_asteroids.write.format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/neo_db") \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", "close_hazardous_asteroids") \
            .option("user", "postgres").option("password", "admin").save()
    

    # Find the percentage of Small Bodies that are Near Earth Objects
    total_rows = df.count()
    neo_true_count = df.filter(df.neo == 'Y').count()
    neo_true_percent = (neo_true_count / total_rows) * 100

    print("Percentage of Small bodies that are NEOs:", neo_true_percent)
    print("Percentage of Small bodies that are not NEOs", 100 - neo_true_percent)

if __name__ == '__main__':
    main()
