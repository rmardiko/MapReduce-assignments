import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class ExtractHourlyCountsEgypt extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(ExtractHourlyCountsEgypt.class);

  // Mapper: emits (token, 1) for every word occurrence.
  private static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    // Reuse objects to save overhead of object creation.
    private final static IntWritable ONE = new IntWritable(1);
    private final static Text WORD = new Text();
    private final static SimpleDateFormat dateRead = 
        new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
    private final static SimpleDateFormat dateWrite = 
        new SimpleDateFormat("M/dd HH"); //new SimpleDateFormat("yyyyMMddHH");

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      
      // parse the date
      String[] tokens = line.split("\t");
 
      
      try {
        
        if(!tokens[3].isEmpty()) {
          String tweet = tokens[3].toLowerCase(Locale.ENGLISH);
          if (tweet.contains("egypt") || tweet.contains("cairo")) {
            WORD.set(dateWrite.format(dateRead.parse(tokens[1])));
            context.write(WORD, ONE);
          }
          
        }
        
      } catch (Exception e) {
        // Just skip
      }
    }
  }

  // Reducer: sums up all the counts.
  private static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    // Reuse objects.
    private final static IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
      Iterator<IntWritable> iter = values.iterator();
      int sum = 0;
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  /**
   * Creates an instance of this tool.
   */
  public ExtractHourlyCountsEgypt() {}

  /**
   * Runs this tool.
   */
  public int run(String[] args) throws Exception {
    
    String inputPath = "/user/shared/tweets2011/tweets2011.txt";
    String outputPath = "rmardiko-egypt";
    int reduceTasks = 5;

    LOG.info("Tool: " + ExtractHourlyCountsEgypt.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(ExtractHourlyCountsEgypt.class.getSimpleName());
    job.setJarByClass(ExtractHourlyCountsEgypt.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapperClass(MyMapper.class);
    job.setCombinerClass(MyReducer.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new ExtractHourlyCountsEgypt(), args);
  }
}
