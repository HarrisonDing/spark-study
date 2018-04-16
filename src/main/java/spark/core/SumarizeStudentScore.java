package spark.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

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

public class SumarizeStudentScore implements Serializable {
	private static final long	serialVersionUID	= -4278180339277690072L;
	private String				filename;
	private SparkConf			conf;

	public void initSpark(String afile) {
		filename = afile;
		conf = new SparkConf().setMaster("local").setAppName("SumarizeStudentScore");
	}

	@SuppressWarnings("serial")
	public void runJob() {
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> lines = sc.textFile(filename, 1);
		JavaRDD<String> filteredLines = lines.filter(new Function<String, Boolean>() {

			@Override
			public Boolean call(String v1) throws Exception {
				return !v1.contains("Score");
			}
		}).cache();

		JavaRDD<Tuple2<String, Float>> subScoreflatMap = filteredLines
				.flatMap(new FlatMapFunction<String, Tuple2<String, Float>>() {

					@Override
					public Iterator<Tuple2<String, Float>> call(String t) throws Exception {
						String[] ts = t.split("\t");
						Tuple2<String, Float> tuple = new Tuple2<String, Float>(ts[1], Float.valueOf(ts[2]));
						return Arrays.asList(tuple).iterator();
					}
				});
		subScoreflatMap.foreach(f -> {
			System.out.println(f._1 + " -> " + f._2);
		});
		JavaPairRDD<String, Float> subScoreMap = subScoreflatMap
				.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {

					@Override
					public Tuple2<String, Float> call(Tuple2<String, Float> t) throws Exception {
						return t;
					}
				});
		JavaPairRDD<String, Float> subScoreMapReduceByKey = subScoreMap
				.reduceByKey(new Function2<Float, Float, Float>() {

					@Override
					public Float call(Float v1, Float v2) throws Exception {
						return v1 + v2;
					}
				});
		Map<String, Long> subScoreCountByKey = subScoreMap.countByKey();
		subScoreMapReduceByKey.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {

			@Override
			public Tuple2<String, Float> call(Tuple2<String, Float> t) throws Exception {
				Float avg = t._2 / subScoreCountByKey.get(t._1);
				return new Tuple2<String, Float>(t._1, avg);
			}
		}).foreach(new VoidFunction<Tuple2<String, Float>>() {

			@Override
			public void call(Tuple2<String, Float> t) throws Exception {
				System.out.println("Target Subject - " + t._1 + " => " + t._2);
			}
		});

		subScoreMapReduceByKey.foreach(new VoidFunction<Tuple2<String, Float>>() {

			@Override
			public void call(Tuple2<String, Float> t) throws Exception {
				System.out.println("Subject-" + t._1 + ": " + t._2);
			}
		});

		sc.close();
	}
}
