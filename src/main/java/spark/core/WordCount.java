package spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {
	private String filename;
	private SparkConf conf;
	
	
	@SuppressWarnings("serial")
	public void runJob() {
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// read text file to RDD
        JavaRDD<String> lines = sc.textFile(filename, 1);
        sc.textFile(filename, 1);
        
        // flatMap each line to words in the line
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
        JavaPairRDD<String, Integer> mapRDD = words.mapToPair(word -> new Tuple2<>(word, 1));
        JavaPairRDD<String, Integer> reduceRDD = mapRDD.reduceByKey((a, b) -> a + b);
        
        System.out.println("=========  collect words from RDD for printing  =============");
        // collect words from RDD for printing
        for(String word:words.collect()){
            System.out.println(word);
        }
        
        System.out.println("=========  print reduce results  =============");
        // print reduce results
        reduceRDD.foreach(data -> {
			System.out.println("Key: " + data._1 + ", Value: " + data._2);
        }); 
	}
	
	public void initSpark(String afile) {
		filename = afile;
		conf = new SparkConf().setMaster("local").setAppName("WordCount");
	}
}
