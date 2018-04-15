package spark.opt;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

/**
 * @description To optimize data skew with reduce join => map join.
 * 
 * @create April 15, 2018
 * @author Harrison.Ding
 *
 */

public class DataSkew5Optimization implements Serializable {
	private static final long	serialVersionUID	= -8719986034709204559L;
	private String				filea;
	private String				fileb;
	private SparkConf			conf;

	@SuppressWarnings("serial")
	public void runJob() {
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read text file to RDD
		JavaRDD<String> linea = sc.textFile(filea, 1);
		JavaRDD<String> lineb = sc.textFile(fileb, 1);

		List<String> collecta = linea.collect();
		List<String> collectb = lineb.collect();
		List<Tuple2<String, String>> lista = new ArrayList<Tuple2<String, String>>();
		List<Tuple2<String, String>> listb = new ArrayList<Tuple2<String, String>>();

		for (String stra : collecta) {
			String[] sa = stra.split(" ");
			lista.add(new Tuple2<String, String>(sa[0], sa[1]));
		}

		for (String strb : collectb) {
			String[] sb = strb.split(" ");
			listb.add(new Tuple2<String, String>(sb[0], sb[1]));
		}

		JavaRDD<Tuple2<String, String>> list1RDD = sc.parallelize(lista);
		JavaRDD<Tuple2<String, String>> list2RDD = sc.parallelize(listb);
		List<Tuple2<String, String>> rdd1data = list1RDD.collect();
		final Broadcast<List<Tuple2<String, String>>> rdd1braodcast = sc.broadcast(rdd1data);
		JavaPairRDD<String, Tuple2<String, String>> resultRDD = list2RDD
				.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<String, String>>() {

					public Tuple2<String, Tuple2<String, String>> call(Tuple2<String, String> t) throws Exception {
						List<Tuple2<String, String>> rdd1data = rdd1braodcast.value();
						Map<String, String> rdd1dataMap = new HashMap<String, String>();
						for (Tuple2<String, String> data : rdd1data) {
							rdd1dataMap.put(data._1, data._2);
						}
						// rdd2 key value
						String key = t._1;
						String value = t._2;
						String rdd1value = rdd1dataMap.get(key);
						return new Tuple2<String, Tuple2<String, String>>(key,
								new Tuple2<String, String>(value, rdd1value));
					}
				});

		resultRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {

			public void call(Tuple2<String, Tuple2<String, String>> t) throws Exception {
				System.out.println(t._1 + "  " + t._2._1 + "  " + t._2._2);

			}
		});

		/*
		 * JavaPairRDD<String, Tuple2<String, String>> resultPairRDD = flatMapbRDD
		 * .mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<String,
		 * String>>() {
		 * 
		 * @Override public Tuple2<String, Tuple2<String, String>> call(Tuple2<String,
		 * String> t) throws Exception { List<Tuple2<String, String>> rddaData =
		 * rddaBroadCast.value(); HashMap<String, String> rddaDataMap = new
		 * HashMap<String, String>(); for (Iterator iterator = rddaData.iterator();
		 * iterator.hasNext();) { Tuple2<String, String> tuple2 = (Tuple2<String,
		 * String>) iterator.next(); rddaDataMap.put(tuple2._1, tuple2._2); } // From
		 * RDD B String key = "DD"; // String key = t._1; // String value = t._2; String
		 * value = "EE"; String rddavalue = "DFDF"; // String rddavalue =
		 * rddaDataMap.get(key); System.out.println("key:  " + key + ", value: " + value
		 * + ", rddavalue： " + rddavalue); return new Tuple2<String, Tuple2<String,
		 * String>>(key, new Tuple2<String, String>(value, rddavalue)); } });
		 * 
		 * System.out.println("\n=========  print reduce final results  =============");
		 * resultPairRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String,
		 * String>>>() {
		 * 
		 * @Override public void call(Tuple2<String, Tuple2<String, String>> t) throws
		 * Exception { System.out.println("学号:  " + t._1 + ", 班级: " + t._2._1 + ", 姓名： "
		 * + t._2._2); } });
		 */
		/*
		 * resultPairRDD.foreach(data -> { System.out.println("学号:  " + data._1 +
		 * ", 班级: " + data._2._1 + ", 姓名： " + data._2._2); });
		 */
	}

	public void initSpark(String afile, String bfile) {
		filea = afile;
		fileb = bfile;
		conf = new SparkConf().setMaster("local").setAppName("DataSkewOptimization");
	}
}
