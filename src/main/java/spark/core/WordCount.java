package spark.core;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class WordCount {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("WordCount");
		//SparkContext sc = new SparkContext(conf);
		JavaSparkContext sc = new JavaSparkContext(conf);
		String path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study\\data\\hello.txt";

		// read text file to RDD
        JavaRDD<String> lines = sc.textFile(path, 1);
        sc.textFile(path, 1);
        
        // flatMap each line to words in the line
        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator()); 
        
        // collect RDD for printing
        for(String word:words.collect()){
            System.out.println(word);
        }
        
        while(true) ;
	}

}
