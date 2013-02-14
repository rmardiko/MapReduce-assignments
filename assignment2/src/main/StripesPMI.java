import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import cern.colt.Arrays;

import edu.umd.cloud9.io.map.HMapSIW;
import edu.umd.cloud9.io.pair.PairOfStrings;
import edu.umd.cloud9.util.map.MapKI;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  // Another Mapper with PairOfStrings
  private static class MyStripesPMIMapper extends Mapper<LongWritable, Text, Text, HMapSIW> {
    
    // Reuse objects to save overhead of object creation.
    private static final HMapSIW MAP = new HMapSIW();
    private static final Text KEYWORD = new Text();
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = ((Text) value).toString();

      Set<String> words = new HashSet<String>();
      Collections.addAll(words, line.split("\\s+"));
      String[] tokens = words.toArray(new String[0]);
      
      for (int ii = 0; ii < tokens.length; ii++) {
        String first = tokens[ii];
        
        if (!first.isEmpty()) {
          
          MAP.clear();
          
          // add count 1 for the key, will be used for marginal count
          MAP.increment(first);
          
          for (int jj = 0; jj < tokens.length; jj++) {
            
            if (ii == jj) continue;
            
            // removing duplicates
            if(first.equals(tokens[jj]))
              tokens[jj] = "";
            
            if (!tokens[jj].isEmpty()) {
              MAP.increment(tokens[jj]);
            }
          }
          
          KEYWORD.set(first);
          context.write(KEYWORD, MAP);
        }
      }
    }
  }
  
  private static class MySecondStripesPMIMapper 
     extends Mapper<LongWritable, Text, PairOfStrings, FloatWritable> 
  {
    private final static PairOfStrings PAIR = new PairOfStrings();
    private final static FloatWritable VALUE = new FloatWritable(1);
    
    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      String line = value.toString();
      
      // tokens[0] -> (left,
      // tokens[1] -> right)
      // tokens[2] -> value
      String[] tokens = line.split("\\s+");
      
      String left = tokens[0].substring(1, tokens[0].length()-1);
      String right = tokens[1].substring(0, tokens[1].length()-1);
      
      if (right.equals("*")) {
        PAIR.set(left, right);
        VALUE.set(Float.parseFloat(tokens[2]));
        
        context.write(PAIR,VALUE);
        
      } else {
        // switch the position of words in the pair
        PAIR.set(right, left);
        VALUE.set(Float.parseFloat(tokens[2]));
      
        // emit the new (key,value)
        context.write(PAIR,VALUE);
        
      }
    }
  }

  private static class MyStripesPMICombiner extends Reducer<Text, HMapSIW, Text, HMapSIW> {

    // Reuse objects.
     private final static HMapSIW ACCMAP = new HMapSIW();
     private final static Text KEYWORD = new Text();

     @Override
     public void reduce(Text key, Iterable<HMapSIW> values, Context context)
        throws IOException, InterruptedException {
      
       // Accumulate counts from different maps
       Iterator<HMapSIW> iter = values.iterator();
       ACCMAP.clear();
       
       while (iter.hasNext()) {
         ACCMAP.plus(iter.next());
       }
       
       KEYWORD.set(key.toString());
       context.write(KEYWORD, ACCMAP);
     }
   }
  
  private static class MySecondStripesPMICombiner extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

    // Reuse objects.
     private final static FloatWritable SUM = new FloatWritable();

     @Override
     public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
      // Sum up values.
       Iterator<FloatWritable> iter = values.iterator();
       float sum = 0;
       while (iter.hasNext()) {
         sum += iter.next().get();
       }
       SUM.set(sum);
       context.write(key, SUM);
     }
   }
  
  //First Reducer: sums up all the counts.
  private static class MyStripesPMIReducer extends Reducer<Text, HMapSIW, PairOfStrings, FloatWritable> {
    
    // Reuse objects.
    private final static HMapSIW ACCMAP = new HMapSIW();
    private final static PairOfStrings KEYWORD = new PairOfStrings();
    private final static FloatWritable VALUE = new FloatWritable();

    @Override
    public void reduce(Text key, Iterable<HMapSIW> values, Context context)
       throws IOException, InterruptedException {
     
      // Accumulate counts from different maps
      Iterator<HMapSIW> iter = values.iterator();
      ACCMAP.clear();
      
      while (iter.hasNext()) {
        ACCMAP.plus(iter.next());
      }
      
      String stringKey = key.toString();
      
      float marginal = (float) ACCMAP.get(stringKey);
      
      // exclude counts that are less than 10
      if (marginal >= 10) {
        
        KEYWORD.set(stringKey, "*");
        VALUE.set(marginal);
        context.write(KEYWORD, VALUE);
        
        ACCMAP.remove(stringKey);
        
        // process all entries
        for (MapKI.Entry<String> entry : ACCMAP.entrySet()) {
          
          // check if the count is greater than 10
          if (entry.getValue() >= 10) {
            
            KEYWORD.set(stringKey, entry.getKey());
            VALUE.set( ((float)entry.getValue()) / marginal);
            //VALUE.set( (float) entry.getValue() );
            
            context.write(KEYWORD, VALUE);
          }
          
        }
      }
    }
  }
  
  private static class MySecondStripesPMIReducer extends Reducer<PairOfStrings, FloatWritable, PairOfStrings, FloatWritable> {

    // Reuse objects.
     private final static FloatWritable VALUE = new FloatWritable();
     private float marginal = 0.0f;

     @Override
     public void reduce(PairOfStrings key, Iterable<FloatWritable> values, Context context)
        throws IOException, InterruptedException {
       
       // No need to sum up values because all (key,value) pairs are unique
       Iterator<FloatWritable> iter = values.iterator();
       float sum = 0;
       while (iter.hasNext()) {
         sum += iter.next().get();
       }
       
       // Marginal count for the second event p(b)
       if (key.getRightElement().equals("*")) {
         //VALUE.set(sum);
         
         // Do not write marginals in the final output
         //context.write(key, VALUE);
         marginal = sum;
       } else {
         VALUE.set(sum / marginal);
         context.write(key, VALUE);
       }
     }
   }

  /**
   * Creates an instance of this tool.
   */
  public StripesPMI() {}

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String NUM_REDUCERS = "numReducers";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of reducers").create(NUM_REDUCERS));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(INPUT) || !cmdline.hasOption(OUTPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String inputPath = cmdline.getOptionValue(INPUT);
    String outputPath = cmdline.getOptionValue(OUTPUT);
    int reduceTasks = cmdline.hasOption(NUM_REDUCERS) ?
        Integer.parseInt(cmdline.getOptionValue(NUM_REDUCERS)) : 1;

    long startTime = System.currentTimeMillis();
    
    if(firstTask(inputPath, "pmi_temp", reduceTasks)) {
      
      if(secondTask("pmi_temp", outputPath, reduceTasks)) {
        
        LOG.info("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
        
      }
      
    }

    return 0;
  }
  
  private boolean firstTask(String inputPath, String outputPath, int reduceTasks) throws Exception {
    
    LOG.info("Tool: First phase of StripesPMI");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName() + " - 1st phase");
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));
    
    FileOutputFormat.setCompressOutput(job, true);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(HMapSIW.class);
    
    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MyStripesPMIMapper.class);
    job.setCombinerClass(MyStripesPMICombiner.class);
    job.setReducerClass(MyStripesPMIReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);
    
    return job.waitForCompletion(true);
  }
  
  private boolean secondTask(String inputPath, String outputPath, int reduceTasks) throws Exception {
    
    LOG.info("Tool: Second phase of StripesPMI");
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - number of reducers: " + reduceTasks);

    Configuration conf = getConf();
    Job job = Job.getInstance(conf);
    job.setJobName(StripesPMI.class.getSimpleName() + " - 2nd phase");
    job.setJarByClass(StripesPMI.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setOutputKeyClass(PairOfStrings.class);
    job.setOutputValueClass(FloatWritable.class);

    job.setMapperClass(MySecondStripesPMIMapper.class);
    job.setCombinerClass(MySecondStripesPMICombiner.class);
    job.setReducerClass(MySecondStripesPMIReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(conf).delete(outputDir, true);
    
    return job.waitForCompletion(true);
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }
}