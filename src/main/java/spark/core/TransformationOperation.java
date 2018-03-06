/**
 * @Filename: TransformationOperation.java
 * @Project: spark-study
 * @Description: 
 * @author: Harrison.Ding
 * @Create: Mar 3, 2018
 */
package spark.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * @author Harrison.Ding
 *
 */

/**
 * Issue and fix:
 * 1. Exception in thread "main" org.apache.spark.SparkException: Task not serializable
 *    If create a class to encapsulate atomic operation
 *  Fixed by : Add to implement from Serializable when create the class
 * 
 * @author Harrison.Ding
 *
 */
public class TransformationOperation implements Serializable {
	private static final long serialVersionUID = 4104787935603750451L;

	public void map() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("Map");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<String> list = Arrays.asList("张无忌", "赵敏", "周芷若");
		JavaRDD<String> listRDD = sc.parallelize(list);
		// R(para 2) - return value
		JavaRDD<String> map = listRDD.map(new Function<String, String>() {
			/**
			 * 
			 */

			public String call(String str) throws Exception {
				return "Hello " + str;
			}
		});
		map.foreach(new VoidFunction<String>() {
			
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
	}

	/**
	 * filter out all odd number from array
	 */
	public void filter() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("Filter");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9);
		JavaRDD<Integer> listRDD = sc.parallelize(list);
		JavaRDD<Integer> filter = listRDD.filter(new Function<Integer, Boolean>() {
			public Boolean call(Integer i) throws Exception {
				return i % 2 == 0;
			}
		});
		
		filter.foreach(new VoidFunction<Integer>() {
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
	}

	/**
	 * simulate an array with spliting into single line
	 */
	public void flatMap() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("flatMap");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<String> list = Arrays.asList("you	jump", "I	jump");
		JavaRDD<String> listRDD = sc.parallelize(list);
		JavaRDD<String> flatMap = listRDD.flatMap(new FlatMapFunction<String, String>() {

			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\t")).iterator();
			}
			
		});
		flatMap.foreach(new VoidFunction<String>() {

			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
	}

	/**
	 * Group by a key
	 */
	public void groupByKey() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("groupByKey");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Tuple2<String, String>> list = Arrays.asList(
			new Tuple2<String, String>("峨嵋", "周芷若"),
			new Tuple2<String, String>("武当", "宋青书"),
			new Tuple2<String, String>("峨嵋", "灭绝师太"),
			new Tuple2<String, String>("武当", "张无忌"));
		JavaPairRDD<String, String> listRDD = sc.parallelizePairs(list);
		JavaPairRDD<String, Iterable<String>> groupByKey = listRDD.groupByKey();
		groupByKey.foreach(new VoidFunction<Tuple2<String,Iterable<String>>>() {

			public void call(Tuple2<String, Iterable<String>> t) throws Exception {
				System.out.println(t._1);
				Iterator<String> iterator = t._2.iterator();
				while(iterator.hasNext()) {
					System.out.println(iterator.next());
				}
				System.out.println("=====group by key======");
			}
		});
	}

	public void reduceByKey() {
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
		JavaRDD<Tuple2<String, Integer>> listRDD = sc.parallelize(list);
		JavaPairRDD<String, Integer> mapToPair = listRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, String, Integer>() {
			public Tuple2<String, Integer> call(Tuple2<String, Integer> t) throws Exception {
				return new Tuple2<String, Integer>(t._1, t._2);
			}
		});
		mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
			
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).foreach(new VoidFunction<Tuple2<String,Integer>>() {

			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1+ "分数: " + t._2);
			}
		});
		
	}

	public void sortByKey() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("sortByKey");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Tuple2<Integer, String>> list = Arrays.asList(
			new Tuple2<Integer, String>(67, "东方不败"),
			new Tuple2<Integer, String>(60, "岳不群"),
			new Tuple2<Integer, String>(44, "令狐冲"),
			new Tuple2<Integer, String>(36, "任我行"));
		JavaPairRDD<Integer, String> listRDD = sc.parallelizePairs(list);
		listRDD.sortByKey().foreach(new VoidFunction<Tuple2<Integer,String>>() {
			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._2 + " : " + t._1);
			}
		});
	}
	
	public void join() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("join");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Tuple2<Integer, String>> listName = Arrays.asList(
			new Tuple2<Integer, String>(1, "东方不败"),
			new Tuple2<Integer, String>(2, "岳不群"),
			new Tuple2<Integer, String>(3, "令狐冲"),
			new Tuple2<Integer, String>(4, "任我行"));
		
		List<Tuple2<Integer, Integer>> listScores = Arrays.asList(new Tuple2<Integer, Integer>(1, 60),
				new Tuple2<Integer, Integer>(2, 70),
				new Tuple2<Integer, Integer>(3, 80),
				new Tuple2<Integer, Integer>(4, 90));
		
		JavaPairRDD<Integer, String> listNameRDD = sc.parallelizePairs(listName);
		JavaPairRDD<Integer, Integer> listScoreRDD = sc.parallelizePairs(listScores);
		
		JavaPairRDD<Integer, Tuple2<String, Integer>> join = listNameRDD.join(listScoreRDD);
		join.foreach(new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

			@Override
			public void call(Tuple2<Integer, Tuple2<String, Integer>> t) throws Exception {
				System.out.println("No: " + t._1 + ", Name: " + t._2()._1
						+ ", Score: " + t._2()._2());
			}
		});
	}

	public void cogroup() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("cogroup");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Tuple2<Integer, String>> listName = Arrays.asList(
			new Tuple2<Integer, String>(1, "东方不败"),
			new Tuple2<Integer, String>(2, "岳不群"),
			new Tuple2<Integer, String>(3, "令狐冲"),
			new Tuple2<Integer, String>(4, "任我行"));
		
		List<Tuple2<Integer, Integer>> listScores = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 60),
				new Tuple2<Integer, Integer>(2, 70),
				new Tuple2<Integer, Integer>(3, 80),
				new Tuple2<Integer, Integer>(4, 90),
				new Tuple2<Integer, Integer>(1, 61),
				new Tuple2<Integer, Integer>(2, 71),
				new Tuple2<Integer, Integer>(3, 81),
				new Tuple2<Integer, Integer>(4, 91));
		
		JavaPairRDD<Integer, String> listNameRDD = sc.parallelizePairs(listName);
		JavaPairRDD<Integer, Integer> listScoreRDD = sc.parallelizePairs(listScores);
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> cogroup = listNameRDD.cogroup(listScoreRDD);
		cogroup.foreach(new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

			@Override
			public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
				System.out.println("No: " + t._1 + ", Name Array: " + t._2._1 + ", Score Array: " + t._2._2);
			}
		});
	}

	public void union() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("union");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Integer> lista = Arrays.asList(1, 2, 3, 4);
		List<Integer> listb = Arrays.asList(4, 5, 6, 7, 8);
		
		
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		JavaRDD<Integer> listbRDD = sc.parallelize(listb);
		JavaRDD<Integer> union = listaRDD.union(listbRDD);
		union.foreach(new VoidFunction<Integer>() {

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
	}

	public void intersection() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("intersection");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Integer> lista = Arrays.asList(1, 2, 3, 4, 6);
		List<Integer> listb = Arrays.asList(4, 5, 6, 7, 8);
		
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		JavaRDD<Integer> listbRDD = sc.parallelize(listb);
		listaRDD.intersection(listbRDD).foreach(new VoidFunction<Integer>() {

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	public void distinct() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("distinct");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Integer> lista = Arrays.asList(1, 2, 3, 4, 6, 4, 5, 6, 7, 8);
		
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		listaRDD.distinct().foreach(new VoidFunction<Integer>() {

			@Override
			public void call(Integer t) throws Exception {
				System.out.println(t);
			}
		});
	}
	
	public void cartesian() {
		// Create a SparkConf object
		SparkConf conf = new SparkConf();
		// setMaster to check run it in local or cluster
		// Default in cluster
		conf.setMaster("local");
		// Set job name
		conf.setAppName("cartesian");
		// create app run entry
		JavaSparkContext sc = new JavaSparkContext(conf);
		// simulate a set to create RDD in parallel way
		List<Integer> lista = Arrays.asList(1, 2, 3, 4);
		List<String> listb = Arrays.asList("a", "b", "c", "d");
		JavaRDD<Integer> listaRDD = sc.parallelize(lista);
		JavaRDD<String> listbRDD = sc.parallelize(listb);
		JavaPairRDD<Integer, String> cartesian = listaRDD.cartesian(listbRDD);
		cartesian.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1 + " " + t._2);
			}
		});
	}

}
