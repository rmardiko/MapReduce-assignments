import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;

public class SampleTest {
  @Test
  public void test1() throws Exception {
    assertEquals(1, Integer.parseInt("1"));
    assertTrue(1 < 2);
  }
  
  @Test
  public void test2() throws IOException {
    VIntWritable aa = new VIntWritable(3);
    VIntWritable bb = new VIntWritable(1000000000);
    
    byte[] aaBytes = WritableUtils.toByteArray(aa);
    byte[] bbBytes = WritableUtils.toByteArray(bb);
    
    System.out.println("aa bytes: " + aaBytes.length + "; " + Integer.toBinaryString(aa.get()));
    System.out.println("bb bytes: " + bbBytes.length + "; " + Integer.toBinaryString(bb.get()));
    
    System.out.println("vintsize aa: " + WritableUtils.getVIntSize((long)aa.get()));
    System.out.println("vintsize bb: " + WritableUtils.getVIntSize((long)bb.get()));
    
    for (int i = 0; i < 6; i++)
       System.out.println(i +": "+bbBytes[i]);
    
    byte[] ccBytes = new byte[6];
    ccBytes[0] = aaBytes[0];
    ccBytes[1] = bbBytes[0];
    ccBytes[2] = bbBytes[1];
    ccBytes[3] = bbBytes[2];
    ccBytes[4] = bbBytes[3];
    ccBytes[5] = bbBytes[4];
    
    System.out.println(WritableComparator.readVInt(ccBytes, 0));
    System.out.println(WritableComparator.readVInt(ccBytes, 1));
    System.out.println(WritableUtils.decodeVIntSize(ccBytes[1]));
    
    BytesWritable bw = new BytesWritable(ccBytes);
    assertTrue(bw.getLength() == 6);
  }
}
