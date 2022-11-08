import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Crime {
    public static void main(String args[]){
        SparkConf conf = new SparkConf().setAppName("CrimeAnalysisDataset").setMaster("local");
        // create Spark Context
        SparkContext context = new SparkContext(conf);
        // create spark Session
        SparkSession sparkSession = new SparkSession(context);
//     SparkSession sparkSession= SparkSession.builder().master("local").appName("CrimeData.csv").getOrCreate();
     String filePath=Crime.class.getResource("CrimeData.csv").getPath();
     Dataset< Row > dataset=sparkSession.sqlContext().read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(filePath);
     dataset.show();
        System.out.println("========== Print Schema ============");
        dataset.printSchema();
        System.out.println("========== Print Data ==============");
        dataset.show();
        System.out.println("========== Print title ==============");
        dataset.select("title").show();
    }
}
