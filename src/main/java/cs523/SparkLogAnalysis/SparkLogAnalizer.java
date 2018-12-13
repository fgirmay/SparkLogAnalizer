package cs523.SparkLogAnalysis;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkLogAnalizer {

	public static void main(String[] args) throws Exception {
	
		
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("logAnalizer").setMaster("local"));

		// Load our input data
		JavaRDD<String> errors = sc.textFile(args[0]);
		
		String startDate = args[2];
		String endDate = args[3];
		
		System.out.println("Start date is " + startDate);
		System.out.println("End date is " + endDate);
		
		JavaRDD<String> errors401 = errors
				.filter(f -> LogRegExp.checkErrorCodeWithInDate(f, startDate, endDate));
		
		System.out.println("The number of 401 errors between " + startDate + " and " + endDate + " is " + errors401.count());
		
		errors401.saveAsTextFile(args[1]);

		sc.close();
	}
}