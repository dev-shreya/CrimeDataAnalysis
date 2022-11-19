import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class Crime implements Serializable {
    public static void main(String args[]){
        SparkConf sparkConf = new SparkConf()
                .setAppName("Crime Data analysis")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
        //


  // SanDiago Data analysis
        JavaRDD<String> sandiegoRDD = sparkContext.textFile("SANDAG_Crime_Data1.csv");
        JavaRDD<String> filteredRDD = sandiegoRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] tokens = s.split(",");

                        int year = new Integer(tokens[0]).intValue();
                        if (year >=2017 && year <=2021) return true;
                        else return false;
                    }
                }
        );

        JavaPairRDD<String,Tuple2<Float,Integer>> sddataRDD = filteredRDD.mapToPair(
                new PairFunction<String, String, Tuple2<Float, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Float, Integer>> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String year= tokens[0];
                        Float crimerate = new Float(tokens[2]);
                        return new Tuple2(year, new Tuple2(crimerate,1));
                    }
                }
        ).repartition(1);


        JavaPairRDD<String, Tuple2<Float,Integer>> resultRDD = sddataRDD.reduceByKey(
                new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
                    @Override
                    public Tuple2<Float, Integer> call(Tuple2<Float, Integer> floatIntegerTuple2, Tuple2<Float, Integer> floatIntegerTuple22) throws Exception {
                        Float sum = floatIntegerTuple2._1 + floatIntegerTuple22._1;
                        Integer count = floatIntegerTuple2._2 + floatIntegerTuple22._2;

                        return new Tuple2(sum, count);
                    }
                }
        );

        JavaRDD<Tuple2<String,Float>> avgcrimeRDD =resultRDD.map(
                new Function<Tuple2<String, Tuple2<Float, Integer>>, Tuple2<String, Float>>() {
                    @Override
                    public Tuple2<String, Float> call(Tuple2<String, Tuple2<Float, Integer>> stringTuple2Tuple2) throws Exception {
                       String key =stringTuple2Tuple2._1;
                        Float avg =stringTuple2Tuple2._2._1 / stringTuple2Tuple2._2._2;

                        return new Tuple2(key,avg);
                    }
                }
        );

//        JavaRDD<Tuple2<String,Float>> sortedSDRDD = avgcrimeRDD.sortBy(
//                new Function<Tuple2<String, Float>, Object>() {
//
//                    @Override
//                    public Object call(Tuple2<String, Float> stringFloatTuple2) throws Exception {
//
//                        return null;
//                    }
//                }
//        );

        JavaRDD<String> finalop = avgcrimeRDD.map(
                new Function<Tuple2<String, Float>, String>() {

                    @Override
                    public String call(Tuple2<String, Float> stringFloatTuple2) throws Exception {

                        String s = stringFloatTuple2._1 + ","+Float.toString(stringFloatTuple2._2);
                        return s;
                    }
                }
        );

        finalop.saveAsTextFile("sand_output");





//        JavaPairRDD<String,String> crimePairRDD = crimeDataRDD.mapToPair(
//                new PairFunction<String, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(String s) throws Exception {
//                        String incident_id= s.split(",")[0];
//
//                        return new Tuple2(incident_id,s); //Key value Pair
//                    }
//                }
//        ).repartition(10);
//
// JavaRDD<String> crimeDataRDD = sparkContext.textFile("CrimeData.csv");
//
//
//        JavaRDD<String> offenceRDD = crimeDataRDD.filter(
//                new Function<String, Boolean>() {
//                    @Override
//                    public Boolean call(String s) throws Exception {
//                        String[] tokens = s.split(",");
//                        int offenceID = new Integer(tokens[0]).intValue();
//                        if (offenceID >= 201000000 && offenceID <= 201340100) return true;
//                        else return false;
//                    }
//                }
//        ).repartition(4);
//
//        crimePairRDD.saveAsTextFile("output");

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
