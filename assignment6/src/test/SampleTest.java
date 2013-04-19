import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

import edu.umd.cloud9.io.array.ArrayOfFloatsWritable;
import edu.umd.cloud9.io.map.HashMapWritable;

public class SampleTest {
  @Test
  public void test1() throws Exception {
    assertEquals(1, Integer.parseInt("1"));
    assertTrue(1 < 2);
  }
  
  @Test
  public void test2() {
    String[] a = "1982121".split(",");
    assertEquals(a.length,1);
  }
  
  @Test
  public void testSplitSample() {
    String input = 
        "28965131991912448\t" + 
        "Sun Jan 23 00:00:00 +0000 2011\t" + 
        "drizzybiebs\t" + 
        "@Biebercrombie haha I'm on my iPod and I'm on the tumblr app and it's working fine now ;)";
    
    assertTrue(input.split("\t").length == 4);
  }
  
  @Test
  public void testParsingDateAndTime() throws Exception {
    SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date dateTime = format1.parse("2012-01-10 23:13:26");
    
    System.out.println(dateTime);
    
    SimpleDateFormat format2 = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
    Date dateTime2 = format2.parse("Sun Jan 23 00:00:00 +0000 2011");
    
    SimpleDateFormat format3 = new SimpleDateFormat("yyyyMMddHH");
    
    System.out.println(format3.format(dateTime2));
    
    Date dateTime3 = format3.parse("2011012219");
    
    SimpleDateFormat format4 = new SimpleDateFormat("M/dd HH");
    System.out.println(format4.format(format3.parse("2011012219")));
  }
  
  @Test
  public void testFindEgyptString() {
    String test = "I dont know what TO write But I want to Have EgYPT";
    
    assertTrue(test.toLowerCase(Locale.ENGLISH).contains("egypt"));
  }
}
