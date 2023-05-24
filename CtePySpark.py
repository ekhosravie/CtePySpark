from pyspark.sql.functions import col

df1 = spark.createDataFrame([(1, 'A'), (2, 'B'), (3, 'C')], ['id', 'category'])
df2 = spark.createDataFrame([(1, 'X'), (2, 'Y'), (3, 'Z')], ['id', 'value'])

df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")

df = spark.sql("""
    WITH cte AS (
        SELECT t1.id, t1.category, t2.value
        FROM table1 t1
        JOIN table2 t2 ON t1.id = t2.id
    )
    SELECT *
    FROM cte
""")

df.show()