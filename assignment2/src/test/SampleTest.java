import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.StringTokenizer;

import org.junit.Test;

public class SampleTest {
  @Test
  public void test1() throws Exception {
    assertEquals(1, Integer.parseInt("1"));
    assertTrue(1 < 2);
  }
  
  @Test
  public void test2() {
    String line = "&c_french\t1";
    String[] words = line.split("\t");
    
    assertEquals(words[0],"&c_french");
    assertEquals(words[1],"1");
  }
  
  @Test
  public void test3() {
    String s = "This is my new string";
    StringTokenizer itr = new StringTokenizer(s);
    
    assertEquals(itr.countTokens(),5);
    
    String[] t = s.split("\\s+");
    assertEquals(t.length, 5);
  }
  
  @Test
  public void test4() {
    String a = "(&c, alcibiades)  1.0";
    assertEquals(a.split("\\s+").length,3);
    
    String b = "(&c,";
    assertEquals(b.substring(1,b.length()-1),"&c");
    
    String[] c = a.split("\\s+");
    assertTrue(Float.parseFloat(c[2])==1.0);
    
    String aa = "(2, iv)  0.45833334";
    String[] cc = aa.split("\\s+");
    System.out.println(Float.parseFloat(cc[2]));
    //assertTrue(Float.parseFloat(cc[2])==0.9791667);
  }
}
