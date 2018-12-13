package cs523.SparkLogAnalysis;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogRegExp {
	
	public static final int NUM_FIELDS = 7;
	
	public static boolean checkErrorCodeWithInDate(String logEntryLine, String start, String end) {
		
		String logEntryPattern = "^([\\d.]+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(.+?)\" (\\d{3}) (\\d+)";

	    Pattern p = Pattern.compile(logEntryPattern);
	    Matcher matcher = p.matcher(logEntryLine);
	    if (!matcher.matches() || 
	      NUM_FIELDS != matcher.groupCount()) {
	      System.err.println("Bad log entry (or problem with RE?):");
	      System.err.println(logEntryLine);
	      return false;
	    }
	    
	    boolean isInRange = false;
	    
	    System.out.println("IP Address: " + matcher.group(1));
	    System.out.println("Date&Time: " + matcher.group(4));
	    System.out.println("Request: " + matcher.group(5));
	    System.out.println("Response: " + matcher.group(6));
	    System.out.println("Bytes Sent: " + matcher.group(7));
	    
	    String date = matcher.group(4).substring(0, 11).replace("/", "-");
	    
		SimpleDateFormat dateFormat = new SimpleDateFormat("dd-MMM-yyyy");
		try {
		
			Date logDate = dateFormat.parse(date);
			Date startDate = dateFormat.parse(start);
		    Date endDate = dateFormat.parse(end);
		    
		     isInRange = logDate.after(startDate) && logDate.before(endDate);
		    
		} catch (ParseException e) {
			e.printStackTrace();
		}
	    
	    
		return "401".equals(matcher.group(6)) && isInRange;
	}
}