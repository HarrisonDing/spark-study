package spark.core;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;
import spark.log.ApacheAccessLog;

public class LogAnalysisCase implements Serializable {
	/**
	 * 
	 */
	private static final long	serialVersionUID	= 4104787935303750451L;
	private String				filename;
	private SparkConf			conf;
	private ApacheAccessLog		apacheAccessLog		= new ApacheAccessLog();

	public void initSpark(String afile) {
		filename = afile;
		conf = new SparkConf().setMaster("local").setAppName("Log Analysis Case");
	}

	public void runJob() {
		JavaSparkContext sc = new JavaSparkContext(conf);

		// read text file to RDD
		JavaRDD<String> lines = sc.textFile(filename, 1);
		JavaRDD<ApacheAccessLog> logsRDD = lines.map(line -> apacheAccessLog.parseLog(line)).cache();

		// Get max, min, average of content size
		List<Long> parseResponseSize = parseResponseSize(logsRDD);
		System.out.println("Get max, min, average of content size: ");
		System.out.println(parseResponseSize);

		// Get each response code happened times
		System.out.println("Get each response code happened times");
		getResponseCount(logsRDD);

		// All IPAddresses that have accessed this server more than N times.
		System.out.println("All IPAddresses that have accessed this server more than N times.");
		getTopNIpAddr(logsRDD, 2);

		// The top endpoints requested by count.
		System.out.println("The top endpoints requested by count.");
		JavaRDD<Tuple2<Integer, String>> topNEndpoint = getTopNEndpoint(logsRDD, 2);
		topNEndpoint.foreach(new VoidFunction<Tuple2<Integer, String>>() {

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println("目标地址 : " + t._2 + "  出现的次数：" + t._1);
			}
		});

		logsRDD.unpersist(true);

		sc.close();
	}

	private List<Long> parseResponseSize(JavaRDD<ApacheAccessLog> logsRDD) {
		List<Long> sizeList = new ArrayList<Long>();
		JavaRDD<Long> logsMap = logsRDD.map(log -> log.getContentSize());
		sizeList.add(logsMap.max(Comparator.naturalOrder()));
		sizeList.add(logsMap.min(Comparator.naturalOrder()));

		Long totalSize = logsMap.reduce((a, b) -> a + b);
		sizeList.add(totalSize / logsMap.count());

		return sizeList;
	}

	private Integer getResponseCount(JavaRDD<ApacheAccessLog> logsRDD) {
		JavaPairRDD<Integer, Integer> logsMapRDD = logsRDD.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1));

		logsMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {

			@Override
			public void call(Tuple2<Integer, Integer> t) throws Exception {
				System.out.println(" 响应状态：" + t._1 + "  出现的次数：" + t._2);
			}
		});
		return 0;
	}

	private String getTopNIpAddr(JavaRDD<ApacheAccessLog> logsRDD, Integer N) {
		JavaPairRDD<String, Integer> logsMapRDD = logsRDD.mapToPair(log -> new Tuple2<>(log.getIpAddress(), 1));
		JavaPairRDD<String, Integer> filterRDD = logsMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				return v1 + v2;
			}
		}).filter(new Function<Tuple2<String, Integer>, Boolean>() {

			@Override
			public Boolean call(Tuple2<String, Integer> v1) throws Exception {
				return v1._2 >= N;
			}
		});
		filterRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println("ip : " + t._1 + "  出现的次数：" + t._2);
			}
		});
		return "";
	}

	private JavaRDD<Tuple2<Integer, String>> getTopNEndpoint(JavaRDD<ApacheAccessLog> logsRDD, Integer N) {
		JavaRDD<Tuple2<Integer, String>> sortBy = logsRDD.mapToPair(log -> new Tuple2<>(log.getEndPoint(), 1))
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				}).map(new Function<Tuple2<String, Integer>, Tuple2<Integer, String>>() {

					@Override
					public Tuple2<Integer, String> call(Tuple2<String, Integer> v1) throws Exception {
						return new Tuple2<Integer, String>(v1._2, v1._1);
					}
				}).sortBy(new Function<Tuple2<Integer, String>, Integer>() {

					@Override
					public Integer call(Tuple2<Integer, String> v1) throws Exception {
						return v1._1;
					}
				}, false, 2);
		return sortBy;
	}
}
