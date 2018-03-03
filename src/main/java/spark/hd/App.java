/**
 * @Filename: App.java
 * @Project: spark-study
 * @Description: 
 * @Author: Harrison.Ding
 * @Create: Mar 3, 2018
 */
package spark.hd;

import spark.core.TransformationOperation;
import spark.core.WordCount;

/**
 * Hello world!
 *
 */
enum RUNT{
	WORDCOUNT, MAP, FILTER, FLATMAP
}
public class App
{
	private static RUNT rt = RUNT.FLATMAP;
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
    		
    	default:
    		break;
    	}
		
    }
}
