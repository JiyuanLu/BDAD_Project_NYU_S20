import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

object analytic {
    def main(args: Array[String]) {
        val sc = new SparkContext()
        val sqlCtx = new SQLContext(sc)
        import sqlCtx._
        import sqlCtx.implicits._

        /* Analytic 1: Comparing Chicago and NYC city employee salaries in 2019 at department level. */
        // load two datasets
        val chicago = sqlCtx.read.option("header", "true").csv("BDAD/data/cleaned/chicago")
        val nyc = sqlCtx.read.option("header", "true").csv("BDAD/data/cleaned/nyc")

        // query code for building correspondence between chicago and nyc department names
        /*
        nyc.registerTempTable("nyc")
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%ADMIN%HEARING%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%ANIMAL%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%AVIATION%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%ELECTION%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%ETHICS%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%BUDGET%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%BUILDINGS%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%BUSINESS%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%CLERK%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%COUNCIL%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%COPA%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%CULTUR%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%DAIS%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%DISAB%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%FAMILY%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%SUPPORT%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%FINANC%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%FIRE%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%HEALTH%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%HOUSING%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%ECON%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%DEV%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%HUMAN%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%RELATIONS%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%RESOURCES%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%INSPECTOR%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%LAW%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%LICENSE%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%MAYOR%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%EMERGENCY%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%POLICE%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%PROCU%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%LIBRARY%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%SAFETY%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%STREET%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%SANITATION%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%TRANSPORT%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%TREASURER%" """).collect.foreach(println)
        sqlCtx.sql(""" SELECT DISTINCT Department FROM nyc WHERE Department LIKE "%WATER%" """).collect.foreach(println)
        */

        // Further clean chicago data w.r.t. department.
        // 1. Remove each department that has no counterpart in NYC.
        // 2. Use standard department names.
        chicago.registerTempTable("chicago")
        val chicago_departments_renamed = sqlCtx.sql(""" SELECT CASE WHEN Department IN ("ADMIN HEARING", 
        "BOARD OF ELECTION", "BUDGET & MGMT", "BUILDINGS", "BUSINESS AFFAIRS", "CITY CLERK", "CITY COUNCIL",
        "CULTURAL AFFAIRS", "FINANCE", "FIRE", "HEALTH", "HOUSING", "HUMAN RELATIONS", "LAW", "MAYOR'S OFFICE", 
        "POLICE", "TRANSPORTN", "TREASURER", "WATER MGMNT") THEN Department WHEN Department = "OEMC" THEN "EMERGENCY" 
        WHEN Department = "STREETS & SAN" THEN "SANITATION" ELSE "N/A" END AS DepartmentName, Title, Salary FROM chicago 
        WHERE Department != "N/A" """)
        chicago_departments_renamed.registerTempTable("chicago_renamed")
        val chicago_departments_cleaned = sqlCtx.sql(""" SELECT * FROM chicago_renamed WHERE DepartmentName != "N/A" """)
        val chicago_departments = chicago_departments_cleaned.select($"DepartmentName").distinct()
        chicago_departments.sort($"DepartmentName").collect.foreach(println)

        // Further clean nyc data w.r.t. department.
        // 1. Remove each department that has no counterpart in Chicago
        // 2. Use standard department names.
        nyc.registerTempTable("nyc")
        val nyc_departments_renamed = sqlCtx.sql(""" SELECT CASE WHEN Department IN ("BOARD OF ELECTION", 
        "CITY CLERK", "CITY COUNCIL", "CULTURAL AFFAIRS") THEN Department WHEN Department LIKE "%HEARNING%" THEN 
        "ADMIN TRAILS AND HEARINGS" WHEN Department LIKE "%BUDGET%" THEN "BUDGET & MGMT" WHEN Department LIKE 
        "%BUILDINGS%" THEN "BUILDINGS" WHEN Department LIKE "%BUSINESS%" THEN "BUSINESS AFFAIRS" WHEN 
        Department = "DEPARTMENT OF FINANCE" OR Department = "FINANCIAL INFO SVCS AGENCY" THEN "FINANCE" WHEN Department = 
        "FIRE DEPARTMENT" THEN "FIRE" WHEN Department LIKE "%HEALTH%" THEN "HEALTH" WHEN Department LIKE "%HOUSING%" THEN
        "HOUSING" WHEN Department LIKE "%HUMAN%" OR Department LIKE "%RELATIONS%" THEN "HUMAN RELATIONS" WHEN Department LIKE "%LAW%"
        THEN "LAW" WHEN Department = "OFFICE OF THE MAYOR" THEN "MAYOR'S OFFICE" WHEN Department LIKE "%EMERGENCY%"
        THEN "EMERGENCY" WHEN Department = "POLICE DEPARTMENT" THEN "POLICE" WHEN Department LIKE "%SANITATION%" 
        THEN "SANITATION" WHEN Department LIKE "%TRANSPORTATION%" THEN "TRANSPORTN" WHEN Department LIKE "%ACTUARY%"
        THEN "TREASURER" WHEN Department LIKE "%WATER%" THEN "WATER MGMNT" ELSE "N/A" END AS DepartmentName, Title, Salary FROM nyc """)
        nyc_departments_renamed.registerTempTable("nyc_renamed")
        val nyc_departments_cleaned = sqlCtx.sql(""" SELECT * FROM nyc_renamed WHERE DepartmentName != "N/A" """)

        val nyc_departments = nyc_departments_cleaned.select($"DepartmentName").distinct()
        nyc_departments.sort($"DepartmentName").collect.foreach(println)

        // Union the two cleaned datasets
        val chicago_with_city_name = chicago_departments_cleaned.withColumn("City", lit("Chicago"))
        val nyc_with_city_name = nyc_departments_cleaned.withColumn("City", lit("NYC"))
        val combined = nyc_with_city_name.union(chicago_with_city_name).selectExpr("City", "DepartmentName", "Title", "cast (Salary as int) Salary")

        // Save cleaned data
        chicago_departments_cleaned.write.option("header", "true").format("csv").save("BDAD/data/cleaned/chicago_departments_cleaned")
        chicago_departments_cleaned.write.saveAsTable("jl11046.chicago_departments_cleaned")

        nyc_departments_cleaned.write.option("header", "true").format("csv").save("BDAD/data/cleaned/nyc_departments_cleaned")
        nyc_departments_cleaned.write.saveAsTable("jl11046.nyc_departments_cleaned")

        combined.write.option("header", "true").format("csv").save("BDAD/data/cleaned/combined")
        combined.write.saveAsTable("jl11046.combined")

        /*******************************************************************************/
        /* Analytic 2: Comparing Chicago and NYC police officer salaries in 2019. */
        // Select records for police department only.
        val chicago_police_officer = chicago_departments_cleaned.where("DepartmentName = 'POLICE' and Title LIKE '%POLICE OFFICER%'")
        val nyc_police_officer = nyc_departments_cleaned.where("DepartmentName = 'POLICE' and Title LIKE '%POLICE OFFICER%'")

        // Calculate some statistics for each dataset
        val chicago_police_officer_stats = chicago_police_officer.select(count("Salary").alias("number"), avg("Salary").alias("average"), stddev_pop("Salary").alias("std"))
        val nyc_police_officer_stats = nyc_police_officer.select(count("Salary").alias("number"), avg("Salary").alias("average"), stddev_pop("Salary").alias("std"))

        // Save cleaned data
        chicago_police_officer.write.option("header", "true").format("csv").save("BDAD/data/cleaned/chicago_police_officer")
        chicago_police_officer.write.saveAsTable("jl11046.chicago_police_officer")
        chicago_police_officer_stats.write.option("header", "true").format("csv").save("BDAD/data/cleaned/chicago_police_officer_stats")
        chicago_police_officer_stats.write.saveAsTable("jl11046.chicago_police_officer_stats")

        nyc_police_officer.write.option("header", "true").format("csv").save("BDAD/data/cleaned/nyc_police_officer")
        nyc_police_officer.write.saveAsTable("jl11046.nyc_police_officer")
        nyc_police_officer_stats.write.option("header", "true").format("csv").save("BDAD/data/cleaned/nyc_police_officer_stats")
        nyc_police_officer_stats.write.saveAsTable("jl11046.nyc_police_officer_stats")

        sc.stop()
    }
}