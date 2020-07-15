import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

object hello {
    def main(args: Array[String]) {
        val sc = new SparkContext()
        println("Hello World.")
        sc.stop()
    }
}