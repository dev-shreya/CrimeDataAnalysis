import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class Crime {
    public static void main(String args[]){
        SparkConf sparkConf = new SparkConf()
                .setAppName("Crime Data analysis")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //
        JavaRDD<String> crimeDataRDD = sparkContext.textFile("CrimeData.csv");

      //  crimeDataRDD.saveAsTextFile("output");

        JavaPairRDD<String,String> crimePairRDD = crimeDataRDD.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String incident_id= s.split(",")[0];

                        return new Tuple2(incident_id,s); //Key value Pair
                    }
                }
        ).repartition(10);;
        crimePairRDD.saveAsTextFile("output");

//        SparkConf conf = new SparkConf().setAppName("CrimeAnalysisDataset").setMaster("local");
//        // create Spark Context
//        SparkContext context = new SparkContext(conf);
//        // create spark Session
//        SparkSession sparkSession = new SparkSession(context);
////     SparkSession sparkSession= SparkSession.builder().master("local").appName("CrimeData.csv").getOrCreate();
//     String filePath=Crime.class.getResource("CrimeData.csv").getPath();
//     Dataset< Row > dataset=sparkSession.sqlContext().read().format("com.databricks.spark.csv").option("header", true).option("inferSchema", true).load(filePath);
//     dataset.show();
//        System.out.println("========== Print Schema ============");
//        dataset.printSchema();
//        System.out.println("========== Print Data ==============");
//        dataset.show();
//        System.out.println("========== Print title ==============");
//        dataset.select("title").show();
    }
}
