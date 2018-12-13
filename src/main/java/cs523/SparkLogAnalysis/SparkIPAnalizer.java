package cs523.SparkLogAnalysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class SparkIPAnalizer {
	
	public static final int NUM_FIELDS = 7;
	
	public static void main(String[] args) {
		
		// Create a Java Spark Context
		JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("ipAnalizer").setMaster("local"));

		// Load our input data
		JavaRDD<String> lines = sc.textFile(args[0]);

		// Calculate word count
		JavaPairRDD<String, Integer> ipCounts = lines
					.map(f -> getIPFromLog(f))
					.mapToPair(w -> new Tuple2<String, Integer>(w, 1))
					.reduceByKey((x, y) -> x + y);
		
		
		// Filter words which occur less than the threshold given by user
		JavaPairRDD<String, Integer> result = ipCounts.filter(f -> f._2 > 20);
		
		System.out.println("The number of ip addresses is " + result.count());
		
		result.saveAsTextFile(args[1]);

		sc.close();
	}
	
	private static String getIPFromLog(String log) {
		
		String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";

	    Pattern p = Pattern.compile(logEntryPattern);
	    Matcher matcher = p.matcher(log);
	    if (!matcher.matches() || 
	      NUM_FIELDS != matcher.groupCount()) {
	      System.err.println("Bad log entry (or problem with RE?):");
	      System.err.println(log);
	      return null;
	    }
	   
	    
	    System.out.println("IP Address: " + matcher.group(1));
	    System.out.println("Date&Time: " + matcher.group(4));
	    System.out.println("Request: " + matcher.group(5));
	    System.out.println("Response: " + matcher.group(6));
	    System.out.println("Bytes Sent: " + matcher.group(7));
	    
	    
		return matcher.group(1);
	}

}
