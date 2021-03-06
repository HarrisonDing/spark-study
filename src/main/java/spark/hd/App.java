/**
 * @Filename: App.java
 * @Project: spark-study
 * @Description: 
 * @Author: Harrison.Ding
 * @Create: Mar 3, 2018
 */
package spark.hd;

import spark.core.ActionOperations;
import spark.core.LogAnalysisCase;
import spark.core.SumarizeStudentScore;
import spark.core.TransformationOperation;
import spark.core.WordCount;
import spark.log.BigLogProcessor;
import spark.opt.DataSkew4Optimization;
import spark.opt.DataSkew5Optimization;
import spark.score.SumarizeStudentScore2;

/**
 * Hello world!
 *
 */
enum RUNT {
	WORDCOUNT, MAP, FILTER, FLATMAP, GROUPBYKEY, REDUCEBYKEY, SORTBYKEY, JOIN, COGROUP, UNION, INTERSECTION, DISTINCT, CARTESIAN, MAPPARTITION, REPARTITION, COALESCE, SAMPLE, AGGREGATEBYKEY, MAPPARTITIONWITHINDEX, REPARTITIONANDSORTWITHINPARTITIONS, ACTION_REDUCE, ACTION_COLLECT, ACTION_TAKE, ACTION_COUNT, ACTION_TAKEORDERED, ACTION_SAVEASTEXTFILE, ACTION_COUNT_BY_KEY, ACTION_TAKE_SAMPLE, INTEGRATED_LOG_CASE, DATA_SKEW_SOLUTION4, DATA_SKEW_SOLUTION5, STUDENT_SCORE, STUDENT_SCORE2, BIG_LOG_PROCESSOR, SUMMARIZE_STUDENT_SCORE
}

public class App {
	private static RUNT						rt		= RUNT.SUMMARIZE_STUDENT_SCORE;
	private static TransformationOperation	trans	= new TransformationOperation();
	private static ActionOperations			acto	= new ActionOperations();
	private static String					path	= "";

	public static void main(String[] args) {
		String cwd = System.getProperty("user.dir");
		System.out.println("cwd: " + cwd);

		switch (rt) {
		case WORDCOUNT:
			// 1. WordCount Test
			path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\hello.txt";
			WordCount wc = new WordCount();
			wc.initSpark(path);
			wc.runJob();
			break;
		case MAP:
			trans.map();
			break;

		case FILTER:
			trans.filter();
			break;

		case FLATMAP:
			trans.flatMap();
			break;

		case GROUPBYKEY:
			trans.groupByKey();
			break;

		case REDUCEBYKEY:
			trans.reduceByKey();
			break;

		case SORTBYKEY:
			trans.sortByKey();
			break;

		case JOIN:
			trans.join();
			break;

		case COGROUP:
			trans.cogroup();
			break;

		case UNION:
			trans.union();
			break;

		case INTERSECTION:
			trans.intersection();
			break;

		case DISTINCT:
			trans.distinct();
			break;

		case CARTESIAN:
			trans.cartesian();
			break;

		case MAPPARTITION:
			trans.mapPartition();
			break;

		case REPARTITION:
			trans.repartition();
			break;

		case COALESCE:
			trans.coalesce();
			break;

		case SAMPLE:
			trans.sample();
			break;

		case AGGREGATEBYKEY:
			trans.aggrateByKey();
			break;

		case MAPPARTITIONWITHINDEX:
			trans.mapPartitionWithIndex();
			break;

		case REPARTITIONANDSORTWITHINPARTITIONS:
			trans.repartitionAndSortWithinPartitions();
			break;

		case ACTION_REDUCE:
			acto.reduce();
			break;

		case ACTION_COLLECT:
			acto.collect();
			break;

		case ACTION_TAKE:
			acto.take();
			break;

		case ACTION_COUNT:
			acto.count();
			break;

		case ACTION_TAKEORDERED:
			acto.takeOrdered();
			break;

		case ACTION_SAVEASTEXTFILE:
			acto.saveAsTextFile();
			break;

		case ACTION_COUNT_BY_KEY:
			acto.countByKey();
			break;

		case ACTION_TAKE_SAMPLE:
			acto.takeSample();
			break;

		case INTEGRATED_LOG_CASE:
			path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\log.txt";
			LogAnalysisCase logAnaCase = new LogAnalysisCase();
			logAnaCase.initSpark(path);
			logAnaCase.runJob();
			break;

		case DATA_SKEW_SOLUTION4:
			path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\data_skew4.txt";
			DataSkew4Optimization dataSkewOpt = new DataSkew4Optimization();
			dataSkewOpt.initSpark(path);
			dataSkewOpt.runJob();
			break;

		case DATA_SKEW_SOLUTION5:
			path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\data_skew5_a.txt";
			String pathb = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\data_skew5_b.txt";
			DataSkew5Optimization dataSkewOpt5 = new DataSkew5Optimization();
			dataSkewOpt5.initSpark(path, pathb);
			dataSkewOpt5.runJob();
			break;

		case STUDENT_SCORE:
			path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\student_subject_scores.txt";
			SumarizeStudentScore sumarizeStudentScore = new SumarizeStudentScore();
			sumarizeStudentScore.initSpark(path, "local");
			sumarizeStudentScore.runJob();
			break;

		// Duplicated with next
		case STUDENT_SCORE2:
			path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study-maven\\data\\student_subject_scores.txt";
			SumarizeStudentScore2 sumarizeStudentScore2 = new SumarizeStudentScore2();
			sumarizeStudentScore2.initSpark(path, "local");
			sumarizeStudentScore2.runJob();
			break;

		case SUMMARIZE_STUDENT_SCORE:
			String path = cwd + "/data/student_subject_scores.txt";
			SumarizeStudentScore2 sumarizeStudentScore1 = new SumarizeStudentScore2();
			sumarizeStudentScore1.initSpark(path, "local");
			sumarizeStudentScore1.runJob();

			break;

		case BIG_LOG_PROCESSOR:
			BigLogProcessor.execute("local");
			break;

		default:
			break;
		}

	}
}
