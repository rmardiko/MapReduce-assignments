import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
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
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import edu.umd.cloud9.io.array.ArrayListWritable;
import edu.umd.cloud9.io.pair.PairOfInts;
import edu.umd.cloud9.io.pair.PairOfStringInt;
import edu.umd.cloud9.io.pair.PairOfWritables;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistribution;
import edu.umd.cloud9.util.fd.Object2IntFrequencyDistributionEntry;
import edu.umd.cloud9.util.pair.PairOfObjectInt;

public class BuildInvertedIndexUncompressed extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(BuildInvertedIndexUncompressed.class);

  private static class MyMapper extends Mapper<LongWritable, Text, PairOfStringInt, IntWritable> {
    private static final PairOfStringInt WORD_DOCID = new PairOfStringInt();
    private static final IntWritable TF_COUNT = new IntWritable();
    private static final Object2IntFrequencyDistribution<String> COUNTS =
        new Object2IntFrequencyDistributionEntry<String>();
    
    @Override
    public void map(LongWritable docno, Text doc, Context context)
        throws IOException, InterruptedException {
      String text = doc.toString();
      COUNTS.clear();

      String[] terms = text.split("\\s+");

      // First build a histogram of the terms.
      for (String term : terms) {
        if (term == null || term.length() == 0) {
          continue;
        }

        COUNTS.increment(term);
      }

      // Emit postings.
      for (PairOfObjectInt<String> e : COUNTS) {
        
        WORD_DOCID.set(e.getLeftElement(), (int) docno.get());
        TF_COUNT.set(e.getRightElement());
        
        context.write(WORD_DOCID, TF_COUNT);
      }
    }
  }

  private static class MyReducer extends
      Reducer<PairOfStringInt, IntWritable, Text, PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>> {
    
    private final static Text TERM = new Text();
    private final static String INIT_TERM = "***EMPTY***";
    private final static IntWritable DF = new IntWritable();
    private final static ArrayListWritable<PairOfInts> POSTINGS = new ArrayListWritable<PairOfInts>();

    @Override
    public void setup(Context context) {
      
      TERM.set(INIT_TERM);
      DF.set(0);
      
    }
    
    @Override
    public void reduce(PairOfStringInt key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      
      Iterator<IntWritable> iter = values.iterator();
      
      if (!TERM.toString().equals(INIT_TERM) && 
          !TERM.toString().equals(key.getLeftElement())) {
        context.write(TERM, 
            new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, POSTINGS));

        POSTINGS.clear();
        DF.set(0);
      }
      
      // we don't actually need while loop here.
      while (iter.hasNext()) {
        POSTINGS.add(new PairOfInts(key.getRightElement(), iter.next().get()));
        DF.set(DF.get() + 1);
      }
      
      TERM.set(key.getLeftElement());
    }
     
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
      context.write(TERM, 
          new PairOfWritables<IntWritable, ArrayListWritable<PairOfInts>>(DF, POSTINGS));
    }
  }

  private BuildInvertedIndexUncompressed() {}

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

    LOG.info("Tool name: " + BuildInvertedIndexUncompressed.class.getSimpleName());
    LOG.info(" - input path: " + inputPath);
    LOG.info(" - output path: " + outputPath);
    LOG.info(" - num reducers: " + reduceTasks);

    Job job = Job.getInstance(getConf());
    job.setJobName(BuildInvertedIndexUncompressed.class.getSimpleName());
    job.setJarByClass(BuildInvertedIndexUncompressed.class);

    job.setNumReduceTasks(reduceTasks);

    FileInputFormat.setInputPaths(job, new Path(inputPath));
    FileOutputFormat.setOutputPath(job, new Path(outputPath));

    job.setMapOutputKeyClass(PairOfStringInt.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(PairOfWritables.class);
    job.setOutputFormatClass(MapFileOutputFormat.class);

    job.setMapperClass(MyMapper.class);
    job.setReducerClass(MyReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(outputPath);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    return 0;
  }

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new BuildInvertedIndexUncompressed(), args);
  }
}
