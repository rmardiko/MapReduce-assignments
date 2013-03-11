import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
  public void test3() {
    String[] values = "1982121,-2.034567".split(",");
    System.out.format("%.4f %s%n", Math.exp(Double.parseDouble(values[1])), values[0]);
  }
  
  @Test
  public void test4() {
//    HashMapWritable<IntWritable, FloatWritable> mass = new HashMapWritable<IntWritable, FloatWritable>();
//    mass.put(new IntWritable(1), new FloatWritable(0.001f));
//    mass.put(new IntWritable(2), new FloatWritable(0.002f));
//    mass.put(new IntWritable(3), new FloatWritable(0.003f));
    float[] a = {0.05f, 2.56f, 9.876f};
    ArrayOfFloatsWritable mass = new ArrayOfFloatsWritable(a); 
    
    System.out.println(mass);
  }
}
