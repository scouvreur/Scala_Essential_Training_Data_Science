import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("DataFrameExercise").getOrCreate()

val df_emps = spark.read.option("header", true).csv("employee.txt")

val df_cr = spark.read.option("header", true).csv("country_region.txt")

val df_dd = spark.read.option("header", true).csv("dept_div.txt")

df_emps.createOrReplaceTempView("employees")

val sqldf_emps = spark.sql("SELECT * FROM employees")

val sqldf_emps_by_dept = spark.sql("SELECT department, COUNT(*) FROM employees GROUP BY department")

val sqldf_emps_by_dept_gender = spark.sql("SELECT department, gender, COUNT(*) FROM employees GROUP BY department, gender")

val sqldf_depts = spark.sql("SELECT DISTINCT department FROM employees")

val sqldf_emps_100 = spark.sql("SELECT * FROM employees WHERE id < 100")

val df_joined = df_emps.join(df_cr, "region_id")

df_joined.show(10, false)

val df_json_dd = spark.read.json("dept_div.json")
