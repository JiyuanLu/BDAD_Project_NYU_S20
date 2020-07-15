import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions._

import scala.collection.mutable.ListBuffer

object profile {
    def main(args: Array[String]) {
        val sc = new SparkContext()
        val sqlCtx = new SQLContext(sc)
        import sqlCtx._
        import sqlCtx.implicits._
        
        var str = new ListBuffer[String]()

        /* Load the seven datasets */
        val chicago = sqlCtx.read.option("header", "true").csv("BDAD/data/Chicago_Payroll.csv")
        val chicago_cleaned = sqlCtx.read.option("header", "true").csv("BDAD/data/cleaned/chicago")
        val nyc_employee = sqlCtx.read.option("header", "true").csv("BDAD/data/NYC_Payroll.csv")
        val nyc_employee_cleaned = sqlCtx.read.option("header", "true").csv("BDAD/data/cleaned/nyc")
        val nyc_employee_history = sqlCtx.read.option("header", "true").csv("BDAD/data/cleaned/nyc_history")
        val nyc_job = sqlCtx.read.option("header", "true").csv("BDAD/data/NYC_Jobs.csv")
        val nyc_job_cleaned = sqlCtx.read.option("header", "true").csv("BDAD/data/cleaned/nyc_jobs")

        // Count the number of records in the dataset before/after cleaning.
        str += s"${chicago.count} records for original chicago dataset. ${chicago_cleaned.count} records after cleaning."
        str += s"${nyc_employee.count} records for original nyc employee dataset. ${nyc_employee_cleaned.count} records in Year 2019, ${nyc_employee_history.count} records from Year 2014 to 2019 after cleaning."
        str += s"${nyc_job.count} records for original nyc job dataset. ${nyc_job_cleaned.count} records after cleaning."
        
        // The number of records don't match, because we dropped records that don't belong to year 2019 as well as bad records.
        // However, there are still plenty of good records left for processing.

        // Find the range for each column in each dataset.
        // Chicago employee
        val chicago_departments = chicago_cleaned.select($"Department").distinct()
        val chicago_titles = chicago_cleaned.select($"Title").distinct()
        val chicago_salary = chicago_cleaned.select($"Salary".cast("int"))
        val chicago_salary_max = chicago_salary.agg(max($"Salary"))
        val chicago_salary_min = chicago_salary.agg(min($"Salary"))
        val chicago_salary_avg = chicago_salary.agg(avg($"Salary"))
        val chicago_salary_std = chicago_salary.agg(stddev_pop($"Salary"))

        str += s"There are ${chicago_departments.count} city departments, ${chicago_titles.count} city employee titles in chicago in Year 2019."
        str += s"Maximum salary = ${chicago_salary_max.collect()(0)(0)}, minimum salary = ${chicago_salary_min.collect()(0)(0)}, average salary = ${chicago_salary_avg.collect()(0)(0)}, standard deviation of salary = ${chicago_salary_std.collect()(0)(0)} in Chicago in Year 2019."

        // NYC employee
        val nyc_departments = nyc_employee_cleaned.select($"Department").distinct()
        val nyc_titles = nyc_employee_cleaned.select($"Title").distinct()
        val nyc_salary = nyc_employee_cleaned.select($"Salary".cast("int"))
        val nyc_salary_max = nyc_salary.agg(max($"Salary"))
        val nyc_salary_min = nyc_salary.agg(min($"Salary"))
        val nyc_salary_avg = nyc_salary.agg(avg($"Salary"))
        val nyc_salary_std = nyc_salary.agg(stddev_pop($"Salary"))
        
        str += s"There are ${nyc_departments.count} city departments, ${nyc_titles.count} city employee titles in NYC in Year 2019."
        str += s"Maximum salary = ${nyc_salary_max.collect()(0)(0)}, minimum salary = ${nyc_salary_min.collect()(0)(0)}, average salary = ${nyc_salary_avg.collect()(0)(0)}, standard deviation of salary = ${nyc_salary_std.collect()(0)(0)} in NYC in Year 2019."

        // NYC jobs
        val nyc_departments_2 = nyc_job_cleaned.select($"Department").distinct()
        val nyc_titles_2 = nyc_job_cleaned.select($"Title").distinct()
        val nyc_salary_2 = nyc_job_cleaned.select($"Salary".cast("int"))
        val nyc_salary_max_2 = nyc_salary_2.agg(max($"Salary"))
        val nyc_salary_min_2 = nyc_salary_2.agg(min($"Salary"))
        val nyc_salary_avg_2 = nyc_salary_2.agg(avg($"Salary"))
        val nyc_salary_std_2 = nyc_salary_2.agg(stddev_pop($"Salary"))

        str += s"There are ${nyc_departments_2.count} city departments, ${nyc_titles_2.count} city employee titles in NYC Job postings from Year 2011 to Year 2020."
        str += s"Maximum salary = ${nyc_salary_max_2.collect()(0)(0)}, minimum salary = ${nyc_salary_min_2.collect()(0)(0)}, average salary = ${nyc_salary_avg_2.collect()(0)(0)}, standard deviation of salary = ${nyc_salary_std_2.collect()(0)(0)} in NYC Job postings from Year 2011 to Year 2020."

        // NYC history
        val nyc_departments_3 = nyc_employee_history.select($"Department").distinct()
        val nyc_titles_3 = nyc_employee_history.select($"Title").distinct()
        val nyc_salary_3 = nyc_employee_history.select($"Salary".cast("int"))
        val nyc_salary_max_3 = nyc_salary_3.agg(max($"Salary"))
        val nyc_salary_min_3 = nyc_salary_3.agg(min($"Salary"))
        val nyc_salary_avg_3 = nyc_salary_3.agg(avg($"Salary"))
        val nyc_salary_std_3 = nyc_salary_3.agg(stddev_pop($"Salary"))

        str += s"There are ${nyc_departments_3.count} city departments, ${nyc_titles_3.count} city employee titles in NYC from Year 2014 to Year 2019."
        str += s"Maximum salary = ${nyc_salary_max_3.collect()(0)(0)}, minimum salary = ${nyc_salary_min_3.collect()(0)(0)}, average salary = ${nyc_salary_avg_3.collect()(0)(0)}, standard deviation of salary = ${nyc_salary_std_3.collect()(0)(0)} in NYC from Year 2014 to Year 2019."
        
        val rdd = sc.parallelize(str)
        rdd.saveAsTextFile("BDAD/data/profiling")
        
        sc.stop()
    }
}