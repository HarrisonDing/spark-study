/**
 * @Filename: App.java
 * @Project: spark-study
 * @Description: 
 * @Author: Harrison.Ding
 * @Create: Mar 3, 2018
 */
package spark.hd;

import org.apache.spark.util.Benchmark.Case;

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
	REPARTITION, COALESCE
}
public class App
{
	private static RUNT rt = RUNT.COALESCE;
	private static TransformationOperation trans = new TransformationOperation();

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
    		
    	default:
    		break;
    	}
		
    }
}
