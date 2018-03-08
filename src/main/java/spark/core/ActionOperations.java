package spark.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class ActionOperations implements Serializable {
	public void reduce() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		Integer reduce = listRDD.reduce(new Function2<Integer, Integer, Integer>() {
			
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		});
		System.out.println(reduce);
	}
	
	public void collect() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> lista = Arrays.asList(1, 2, 3, 4, 5);
		List<Integer> listb = Arrays.asList(5, 6, 7, 8, 9);
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		JavaRDD<Integer> listbRDD = sc.parallelize(listb);
		JavaRDD<Integer> union = listaRDD.union(listbRDD);
		List<Integer> collect = union.collect();
		for(Integer i : collect) {
			System.out.println(i);
		}
	}

	public void take() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		List<Integer> take = listRDD.take(3);
		for (Integer integer : take) {
			System.out.println(integer);
		}
	}
	
	public void count() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		long count = listRDD.count();
		System.out.println(count);
	}
	
	public void takeOrdered() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> list = Arrays.asList(1, 2, 3, 224, 5, 8, 7);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		List<Integer> takeOrdered = listRDD.takeOrdered(5);
		for (Integer integer : takeOrdered) {
			System.out.println(integer);
		}
		
		List<Integer> top = listRDD.top(3);
		for (Integer integer : top) {
			System.out.println(integer);
		}
	}

	// It is not work in current system environment, test failed
	public void saveAsTextFile() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("reduce");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> lista = Arrays.asList(1, 2, 3, 4, 5);
		List<Integer> listb = Arrays.asList(5, 6, 7, 8, 9);
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		JavaRDD<Integer> listbRDD = sc.parallelize(listb);
		JavaRDD<Integer> union = listaRDD.union(listbRDD);
		union.saveAsTextFile("D://union.txt");
	}
	
	public void countByKey() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("reduceByKey");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Tuple2<String, Integer>> list = Arrays.asList(
			new Tuple2<String, Integer>("峨嵋", 40),
			new Tuple2<String, Integer>("武当", 60),
			new Tuple2<String, Integer>("峨嵋", 83),
			new Tuple2<String, Integer>("武当", 89));
		JavaPairRDD<String, Integer> listRDD = sc.parallelizePairs(list);
		Map<String, Long> countByKey = listRDD.countByKey();
		for (String key: countByKey.keySet()) {
			System.out.println("key: " + key + ", value: " + countByKey.get(key));
		}
	}

	public void takeSample() {
		SparkConf conf = new SparkConf();
		conf.setMaster("local");
		conf.setAppName("takeSample");
		JavaSparkContext sc = new JavaSparkContext(conf);
		List<Integer> list = Arrays.asList(1, 2, 3, 224, 5, 8, 7);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		List<Integer> takeSample = listRDD.takeSample(true, 2);
		for (Integer integer : takeSample) {
			System.out.println(integer);
		}
		System.out.println("");
	}
}
