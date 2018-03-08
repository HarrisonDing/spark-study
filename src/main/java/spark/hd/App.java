/**
 * @Filename: App.java
 * @Project: spark-study
 * @Description: 
 * @Author: Harrison.Ding
 * @Create: Mar 3, 2018
 */
package spark.hd;

import spark.core.ActionOperations;
import spark.core.TransformationOperation;
import spark.core.WordCount;

/**
 * Hello world!
 *
 */
enum RUNT{
	WORDCOUNT, MAP, FILTER, FLATMAP, GROUPBYKEY,
	REDUCEBYKEY, SORTBYKEY, JOIN, COGROUP, UNION,
	INTERSECTION, DISTINCT, CARTESIAN, MAPPARTITION,
	REPARTITION, COALESCE, SAMPLE, AGGREGATEBYKEY, 
	MAPPARTITIONWITHINDEX, REPARTITIONANDSORTWITHINPARTITIONS, 
	ACTION_REDUCE, ACTION_COLLECT, ACTION_TAKE, ACTION_COUNT, 
	ACTION_TAKEORDERED, ACTION_SAVEASTEXTFILE, ACTION_COUNT_BY_KEY,
	ACTION_TAKE_SAMPLE
}
public class App
{
	private static RUNT rt = RUNT.ACTION_TAKE_SAMPLE;
	private static TransformationOperation trans = new TransformationOperation();
	private static ActionOperations acto = new ActionOperations();

    public static void main( String[] args )
    {
    	switch(rt) {
    	case WORDCOUNT:
    		// 1. WordCount Test
    		String path = "D:\\MyWorkSpace\\ResearchingProjects\\spark-study\\data\\hello.txt";
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
    		
    	default:
    		break;
    	}
		
    }
}
