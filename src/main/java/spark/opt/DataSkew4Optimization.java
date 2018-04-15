package spark.opt;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @description To optimize data skew with local and global aggregation.
 * 
 * @create April 15, 2018
 * @author Harrison.Ding
 *
 */

public class DataSkew4Optimization implements Serializable {
	private static final long	serialVersionUID	= -8719986013909204559L;
	private String				filename;
	private SparkConf			conf;

	@SuppressWarnings("serial")
	public void runJob() {
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read text file to RDD
		JavaRDD<String> lines = sc.textFile(filename, 1);
		sc.textFile(filename, 1);

		// flatMap each line to words in the line
		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(s.split(" ")).iterator());
		JavaPairRDD<String, Integer> mapRDD = words.mapToPair(word -> new Tuple2<>(word, 1));
		JavaPairRDD<String, Integer> localMapRDD = mapRDD
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
						int prefix = new Random().nextInt(5);
						return new Tuple2<String, Integer>(prefix + "_" + t._1, t._2);
					}
				});
		JavaPairRDD<String, Integer> localReduceByKey = localMapRDD
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
		System.out.println("\n=========  print reduce results with map after random  =============");
		localReduceByKey.foreach(data -> {
			System.out.println("Key: " + data._1 + ", Value: " + data._2);
		});

		JavaPairRDD<String, Integer> globalMapToPair = localReduceByKey
				.mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {

					@Override
					public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
						return new Tuple2<String, Integer>(t._1.split("_")[1], t._2);
					}
				});

		JavaPairRDD<String, Integer> globalReduceRDD = globalMapToPair.reduceByKey((a, b) -> a + b);

		System.out.println("\n=========  collect words from RDD for printing  =============");
		for (String word : words.collect()) {
			System.out.print(word + " ");
		}
		System.out.println();

		System.out.println("\n=========  print reduce final results  =============");
		globalReduceRDD.foreach(data -> {
			System.out.println("Key: " + data._1 + ", Value: " + data._2);
		});
	}

	public void initSpark(String afile) {
		filename = afile;
		conf = new SparkConf().setMaster("local").setAppName("DataSkewOptimization");
	}
}
