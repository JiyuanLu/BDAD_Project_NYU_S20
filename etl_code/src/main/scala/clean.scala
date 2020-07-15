import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object clean {
    def main(args: Array[String]) {
        val sc = new SparkContext()
        val sqlCtx = new SQLContext(sc)
        import sqlCtx._
        import sqlCtx.implicits._

        /* Load the three datasets */
        val chicago = sqlCtx.read.option("header", "true").csv("BDAD/data/Chicago_Payroll.csv")
        val nyc_employee = sqlCtx.read.option("header", "true").csv("BDAD/data/NYC_Payroll.csv")
        val nyc_job = sqlCtx.read.option("header", "true").csv("BDAD/data/NYC_Jobs.csv")

        /* Clean data for analytic 1: comparing NYC & Chicago */
        val chicago_cleaned = chicago.filter($"Full or Part-Time" === "F").filter($"Salary or Hourly" === "Salary").filter($"Annual Salary" !== "null").select($"Department", $"Job Titles".alias("Title"), $"Annual Salary".alias("Salary").cast("int"))
        chicago_cleaned.write.option("header", "true").format("csv").save("BDAD/data/cleaned/chicago")

        val nyc_cleaned = nyc_employee.filter($"Fiscal Year" === "2019").filter($"Leave Status as of June 30" === "ACTIVE").filter($"Base Salary" !== "null").filter($"Pay Basis" === "per Annum").select($"Agency Name".alias("Department"), $"Title Description".alias("Title"), $"Base Salary".alias("Salary").cast("int"))
        nyc_cleaned.write.option("header", "true").format("csv").save("BDAD/data/cleaned/nyc")

        /* Clean data for analytic 2: observing trend in NYC */
        val nyc_history = nyc_employee.filter($"Leave Status as of June 30" === "ACTIVE").filter($"Base Salary" !== "null").filter($"Pay Basis" === "per Annum").select($"Fiscal Year".alias("Year"), $"Agency Name".alias("Department"), $"Title Description".alias("Title"), $"Base Salary".alias("Salary").cast("int"))
        nyc_history.write.option("header", "true").format("csv").save("BDAD/data/cleaned/nyc_history")
        nyc_history.write.saveAsTable("jl11046.nyc_history")

        val nyc_job_with_year = nyc_job.withColumn("Year", (year(to_timestamp($"Posting Date", "MM/dd/yy"))).cast("string"))
        nyc_job_with_year.registerTempTable("job")
        val nyc_job_cleaned = sqlCtx.sql(""" SELECT Year, Agency AS Department, `Business Title` AS Title, (`Salary Range From` + `Salary Range To`)/2 AS Salary FROM job WHERE `Full-Time/Part-Time indicator` = "F" AND `Salary Frequency` = "Annual" """)
        nyc_job_cleaned.write.option("header", "true").format("csv").save("BDAD/data/cleaned/nyc_jobs")
        nyc_job_cleaned.write.saveAsTable("jl11046.nyc_jobs")
        
        sc.stop()
    }
}
