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
import org.apache.spark.api.java.function.Function2;

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
		JavaPairRDD<Object, Object> logsMapRDD = logsRDD.mapToPair(log -> new Tuple2<>(log.getResponseCode(), 1));

		/*
		 * logsMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
		 * 
		 * @Override public Integer call(Integer v1, Integer v2) throws Exception {
		 * return v1 + v2; } });
		 */
		return 0;
	}

	private String getTopNIpAddr(Integer N) {
		return "";
	}

	private String getTopNEndpoint(Integer N) {
		return "";
	}
}
