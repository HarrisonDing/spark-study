package spark.score;

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

public class SumarizeStudentScore2 implements Serializable {
	private static final long	serialVersionUID	= -4278180339277690072L;
	private String				filename;
	private SparkConf			conf;
	private NameSubjectScore	nss					= new NameSubjectScore();

	public void initSpark(String afile) {
		filename = afile;
		conf = new SparkConf().setMaster("local").setAppName("SumarizeStudentScore2");
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
		});

		JavaRDD<NameSubjectScore> subScoreCacheMap = filteredLines.map(line -> nss.parseLine(line)).cache();
		subScoreCacheMap.foreach(f -> {
			System.out.println("Name: " + f.getName() + ", Subject: " + f.getSubject() + ", Score: " + f.getScore());
		});

		averageScorePerSubject(subScoreCacheMap).foreach(new VoidFunction<Tuple2<String, Float>>() {

			@Override
			public void call(Tuple2<String, Float> t) throws Exception {
				System.out.println("Target Subject - " + t._1 + " => average score: " + t._2);
			}
		});
		sc.close();
	}

	@SuppressWarnings("serial")
	public JavaPairRDD<String, Float> averageScorePerSubject(JavaRDD<NameSubjectScore> jrnss) {
		JavaPairRDD<String, Float> subScoreMapPair = jrnss
				.mapToPair(new PairFunction<NameSubjectScore, String, Float>() {

					@Override
					public Tuple2<String, Float> call(NameSubjectScore t) throws Exception {
						return new Tuple2<String, Float>(t.getSubject(), t.getScore());
					}
				});
		JavaPairRDD<String, Float> subScoreMapReduceByKey = subScoreMapPair
				.reduceByKey(new Function2<Float, Float, Float>() {

					@Override
					public Float call(Float v1, Float v2) throws Exception {
						return v1 + v2;
					}
				});
		Map<String, Long> subScoreCountByKey = subScoreMapPair.countByKey();

		JavaPairRDD<String, Float> mapToPair = subScoreMapReduceByKey
				.mapToPair(new PairFunction<Tuple2<String, Float>, String, Float>() {

					@Override
					public Tuple2<String, Float> call(Tuple2<String, Float> t) throws Exception {
						return new Tuple2<String, Float>(t._1, t._2 / subScoreCountByKey.get(t._1));
					}
				});
		return mapToPair;
	}
}
