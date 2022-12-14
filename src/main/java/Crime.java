import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class Crime implements Serializable {
    public static void main(String args[]){
        SparkConf sparkConf = new SparkConf()
                .setAppName("Crime Data analysis")
                .setMaster("local[*]");  // Delete this line when submitting to a cluster
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        /*-----------------------Start of Sand Diego Data Analysis----------------------*/
        // Use below file path in case files are placed at hdfs and use respective port mentioned in core-site.xml
        JavaRDD<String> sanDiegoRDD = sparkContext.textFile("hdfs://localhost:9820/CrimeDataset/SandDiego_Crime_Data.csv");

        // uncomment and use below line in case you directly want to access file from input folder
        //        JavaRDD<String> sanDiegoRDD = sparkContext.textFile("input/SANDAG_Crime_Data1.csv");


        JavaRDD<String> filteredSDRDD = sanDiegoRDD.filter(
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

        JavaPairRDD<String,Tuple2<Float,Integer>>  sanDiegoDataKVRDD = filteredSDRDD.mapToPair(
                new PairFunction<String, String, Tuple2<Float, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<Float, Integer>> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String year= tokens[0];
                        Float crimeRate = new Float(tokens[2]);

                        return new Tuple2(year, new Tuple2(crimeRate,1));
                    }
                }
        ).repartition(1);


        JavaPairRDD<String, Tuple2<Float,Integer>> SanDiegoReduceKVRDD = sanDiegoDataKVRDD.reduceByKey(
                new Function2<Tuple2<Float, Integer>, Tuple2<Float, Integer>, Tuple2<Float, Integer>>() {
                    @Override
                    public Tuple2<Float, Integer> call(Tuple2<Float, Integer> floatIntegerTuple2, Tuple2<Float, Integer> floatIntegerTuple22) throws Exception {
                        Float sum = floatIntegerTuple2._1 + floatIntegerTuple22._1;
                        Integer count = floatIntegerTuple2._2 + floatIntegerTuple22._2;

                        return new Tuple2(sum, count);
                    }
                }
        );

        JavaRDD<Tuple2<String,Float>> sanDiegoAvgCrimeRDD =SanDiegoReduceKVRDD.map(
                new Function<Tuple2<String, Tuple2<Float, Integer>>, Tuple2<String, Float>>() {
                    @Override
                    public Tuple2<String, Float> call(Tuple2<String, Tuple2<Float, Integer>> stringTuple2Tuple2) throws Exception {
                       String key =stringTuple2Tuple2._1;
                        Float avg =stringTuple2Tuple2._2._1 / stringTuple2Tuple2._2._2;

                        return new Tuple2(key,avg);
                    }
                }
        );

        JavaPairRDD<String,Float> sanDiegoAvgCrimePairRDD = sanDiegoAvgCrimeRDD.mapToPair(
                new PairFunction<Tuple2<String, Float>, String, Float>() {
                    @Override
                    public Tuple2<String, Float> call(Tuple2<String, Float> stringFloatTuple2) throws Exception {
                        return stringFloatTuple2;
                    }
                }
        );

        JavaPairRDD<String, Float> sanDiegoSortedCrimeRDD = sanDiegoAvgCrimePairRDD.sortByKey();

        JavaRDD<String> sanDiegoFinalOpRDD = sanDiegoSortedCrimeRDD.map(
                new Function<Tuple2<String, Float>, String>() {

                    @Override
                    public String call(Tuple2<String, Float> stringFloatTuple2) throws Exception {

                        String s = stringFloatTuple2._1 + ","+Float.toString(stringFloatTuple2._2);
                        return s;
                    }
                }
        );

        sanDiegoFinalOpRDD.saveAsTextFile("SanDiego_output");

/*-----------------------End of Sand Diego Data Analysis----------------------*/


     /*----------------------Start of  LA Data Analysis----------------------*/


        // Use below file path in case files are placed at hdfs and use respective port mentioned in core-site.xml
        JavaRDD<String> laDataRDD = sparkContext.textFile("hdfs://localhost:9820/CrimeDataset/LosAngeles_Crime_Data.csv");

        // uncomment and use below line in case you directly want to access file from input folder
//        JavaRDD<String> laDataRDD = sparkContext.textFile("input/LosAngeles_Crime_Data.csv");

        /*----Filtering LA crime data between year 2017 and 2022 */

        JavaRDD<String> filteredLARDD = laDataRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        int year = 0;

                        if (tokens.length >=6 && tokens[6] != null) {
                            String[] yearStr = tokens[6].split("/");
                            if(yearStr.length > 0 && isNumeric(yearStr[0]))
                                year = new Integer(yearStr[0]).intValue();
                        }
                        if (year >=2017 && year <=2022) return true;
                        else return false;

                    }
                }
        );

/*------------------mapping similar type of crime to one category---*/
        JavaRDD<String> laCrimeTypeRDD = filteredLARDD.map(
                new Function<String, String>() {
                    @Override
                    public String call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String crimeType = tokens[1].toLowerCase();

                        if(crimeType.contains("theft") || crimeType.contains("larceny")) {
                            crimeType = "Larceny";
                        } else if (crimeType.contains("harassment") || crimeType.contains("sex") || crimeType.contains("assault")) {
                            crimeType ="Harassment/Sex Offense";
                        } else if (crimeType.contains("hit") || crimeType.contains("runaway") ) {
                            crimeType ="Hit & Run/runaway";
                        } else if (crimeType.contains("robbery") || crimeType.contains("burglary")) {
                            crimeType ="Burglary";
                        } else if (crimeType.contains("drug") || crimeType.contains("narcotic")) {
                            crimeType ="Drug/Narcotic";
                        } else if (crimeType.contains("vandalism") ) {
                            crimeType ="Vandalism";
                        } else if (crimeType.contains("shot") || crimeType.contains("fired") ||  crimeType.contains("illegal") ) {
                            crimeType ="Shots Fired/Illegal Hunting";
                        } else if (crimeType.contains("domestic") || crimeType.contains("disturbance")) {
                            crimeType ="Domestic Disturbance";
                        }

                        return crimeType;
                    }
                }
        ).repartition(1);

/*----------------Creating key value pair-------------------------------------*/
        JavaPairRDD<String, Integer> laCrimeDataKeyValueRDD = laCrimeTypeRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String crimeType = tokens[0];
                        return new Tuple2(crimeType, 1);

                    }
                }
        );


        JavaPairRDD<String, Integer> laReducedKeyRDD =laCrimeDataKeyValueRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        Integer occurrence = integer +integer2;
                        return occurrence;
                    }
                }
        );

        JavaPairRDD<Integer, String> laSwappedKVRDD = laReducedKeyRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        });



        List<Tuple2<Integer , String>> sortedListRDD = laSwappedKVRDD.sortByKey(false).take(5);

        JavaPairRDD<Integer, String> laSortedPairRDD = sparkContext.parallelizePairs(sortedListRDD).repartition(1);

        JavaPairRDD<String, Integer> laSwappedVKPairRDD = laSortedPairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String , Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }

        });

        JavaRDD<String> laFinalOpRDD = laSwappedVKPairRDD.map(
                new Function<Tuple2<String, Integer>, String>() {
                    @Override
                    public String call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        String s = stringIntegerTuple2._1 + "," +stringIntegerTuple2._2;
                        return s;
                    }
                }
        );

        laFinalOpRDD.saveAsTextFile("LA_output");


        /*-----------------------End of LA Data Analysis----------------------*/

        //Maryland crime data analysis

        // Use below file path in case files are placed at hdfs and use respective port mentioned in core-site.xml
        JavaRDD<String> marylandRDD = sparkContext.textFile("hdfs://localhost:9820/CrimeDataset/Maryland_Crime_Data.csv");

        // uncomment and use below line in case you directly want to access file from input folder
//        JavaRDD<String> marylandRDD = sparkContext.textFile("input/Maryland_Crime_Data.csv");

        //       --------------------------- filtering the data on basis of year----------------------------------
        JavaRDD<String> filterRDD = marylandRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String[] date = tokens[3].split(" ");
                        String[] y1= date[0].split("/");
                        int year = new Integer(y1[2]).intValue();

                        if (year >=2020 && year <=2022){

                            return true;
                        }

                        else return false;
                    }
                }
        );

//     --------------------Pairing word with count-------------------------------------------
        JavaPairRDD<String, Integer> MdcitydataRDD = filterRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String city= tokens[9].trim();
                        String[] date = tokens[3].split(" ");
                        String[] y1= date[0].split("/");
                        String year =y1[2];
                        return new Tuple2(city, 1);

                    }
                }
        ).repartition(1);

//        --------------------------adds pairs of same word with count--------------
        JavaPairRDD<String, Integer> reducedRDD =MdcitydataRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer num1, Integer num2) throws Exception {
                        Integer count = num1 +num2;
                        return count;
                    }
                }
        );

        JavaPairRDD<Integer, String> MarylandSwapRDD = reducedRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        });
        //        ---------------------------sorting the data and displaying top 5------------------------
        List<Tuple2<Integer , String>> sortedMarylandListRDD = MarylandSwapRDD.sortByKey(false).take(5);


//-----------------------------Creating parallelized collection-----------------------------------
        JavaPairRDD<Integer, String> MDSortedPairRDD = sparkContext.parallelizePairs(sortedMarylandListRDD).repartition(1);

        JavaPairRDD<String, Integer> MDSwappedPairRDD = MDSortedPairRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String , Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }

        });

        JavaRDD<String> MarylandFinalRDD = MDSwappedPairRDD.map(
                new Function<Tuple2<String, Integer>, String>() {
                    @Override
                    public String call(Tuple2<String, Integer> stringIntTuple2) throws Exception {
                        String str = stringIntTuple2._1 + "," +stringIntTuple2._2;
                        return str;
                    }
                }
        );

//        ----------------------Saving the data in output file-----------------------------

        MarylandFinalRDD.saveAsTextFile("Maryland_output");


//        ------------------------- End of MaryLand Data Analysis-------------------------------

        //DC crime data analysis

        // Use below file path in case files are placed at hdfs and use respective port mentioned in core-site.xml
        JavaRDD<String> DCRDD = sparkContext.textFile("hdfs://localhost:9820/CrimeDataset/DC_Crime_Data.csv");

// uncomment and use below line in case you directly want to access file from input folder
//        JavaRDD<String> DCRDD = sparkContext.textFile("input/DC_Crime_Data.csv");
//
            JavaRDD<String> fRDD = DCRDD.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String[] date = tokens[4].split("-");
                        String year = date[0];
//                        if (year==2016) {
                        return true;
//                        } else return false;
                    }
//                        return true;
                }
        );
//     --------------------Pairing word with count-------------------------------------------
        JavaPairRDD<String, Integer> DCdataRDD = fRDD.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        String[] tokens = s.split(",");
                        String timeOfDay = tokens[5];
                        return new Tuple2(timeOfDay, 1);

                    }
                }
        ).repartition(1);

//        --------------------------adds pairs of same word with count--------------
        JavaPairRDD<String, Integer> reduceRDD =DCdataRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer val1, Integer val2) throws Exception {
                        Integer count = val1 +val2;
                        return count;
                    }
                }
        );
        JavaPairRDD<Integer, String> DCSwapRDD = reduceRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> item) throws Exception {
                return item.swap();
            }

        });
//        ---------------------------sorting the data and displaying top 5------------------------
        List<Tuple2<Integer , String>> SortedDCListRDD = DCSwapRDD.sortByKey(false).take(5);

//-----------------------------Creating parallelized collection-----------------------------------
        JavaPairRDD<Integer, String> SortedPairDCRDD = sparkContext.parallelizePairs(SortedDCListRDD).repartition(1);

        JavaPairRDD<String, Integer> SwappedPairDCRDD = SortedPairDCRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String , Integer> call(Tuple2<Integer, String> item) throws Exception {
                return item.swap();
            }

        });

        JavaRDD<String> DCFinalOutputRDD = SwappedPairDCRDD.map(
                new Function<Tuple2<String, Integer>, String>() {
                    @Override
                    public String call(Tuple2<String, Integer> StrIntTuple) throws Exception {
                        String str = StrIntTuple._1 + "," +StrIntTuple._2;
                        return str;
                    }
                }
        );
//        ----------------------Saving the data in output file-----------------------------
        DCFinalOutputRDD.saveAsTextFile("DC_output");

        //        ------------- End of DC Data Analysis-------------------------------



    }

    public static boolean isNumeric(String str) {
        try {
            Double.parseDouble(str);
            return true;
        } catch(NumberFormatException e){
            return false;
        }
    }
}
