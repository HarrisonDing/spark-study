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
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

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

			@Override
			public String call(String str) throws Exception {
				return "Hello " + str;
			}
		});
		map.foreach(new VoidFunction<String>() {
			
			@Override
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
			@Override
			public Boolean call(Integer i) throws Exception {
				return i % 2 == 0;
			}
		});
		
		filter.foreach(new VoidFunction<Integer>() {
			@Override
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

			@Override
			public Iterator<String> call(String t) throws Exception {
				return Arrays.asList(t.split("\t")).iterator();
			}
		});
		flatMap.foreach(new VoidFunction<String>() {

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});
		
	}

}
