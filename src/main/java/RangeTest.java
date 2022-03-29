
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


public class RangeTest {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("CreateTable")
                .getOrCreate();
        Dataset<Long> data = spark.range(5, 10);
        data.show();
    }
}
