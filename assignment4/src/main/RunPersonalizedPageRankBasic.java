/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */



import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import edu.umd.cloud9.io.array.ArrayListOfIntsWritable;
import edu.umd.cloud9.io.array.ArrayOfFloatsWritable;
import edu.umd.cloud9.mapreduce.lib.input.NonSplitableSequenceFileInputFormat;

/**
 * <p>
 * Main driver program for running the basic (non-Schimmy) implementation of
 * PageRank.
 * </p>
 *
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  // Mapper, no in-mapper combining.
  private static class MapClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {

    // The neighbor to which we're sending messages.
    private static final IntWritable neighbor = new IntWritable();

    // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateMass = new PageRankNode();

    // For passing along node structure.
    private static final PageRankNode intermediateStructure = new PageRankNode();
    
    private ArrayListOfIntsWritable sourceIds = new ArrayListOfIntsWritable();
    
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      
      String[] temp = conf.get("SourceNodes").split(",");
      for (int i = 0; i < temp.length; i++) {
        sourceIds.add(Integer.parseInt(temp[i]));
      }
      
      intermediateMass.setSourceIds(sourceIds);
      
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacencyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacencyList().size() > 0) {
        
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacencyList();
        float logOfNumNodes = (float) StrictMath.log(list.size());

        context.getCounter(PageRank.edges).increment(list.size());

        // Iterate over neighbors.
        float mass;
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          
          for (int sourceNodeId : sourceIds) {
            mass = node.getPersonalizedPageRank(sourceNodeId) - logOfNumNodes;            
            intermediateMass.setPersonalizedPageRank(sourceNodeId, mass);
          }
          
          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();
    private ArrayListOfIntsWritable sourceIds = new ArrayListOfIntsWritable();
    private float[] allMass;
    
    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      
      String[] temp = conf.get("SourceNodes").split(",");
      for (int i = 0; i < temp.length; i++) {
        int nid = Integer.parseInt(temp[i]);
        sourceIds.add(nid);
      }
      
      allMass = new float[sourceIds.size()];
      
    }
    
    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context)
        throws IOException, InterruptedException {
      int massMessages = 0;

      // Remember, PageRank mass is stored as a log prob.
      for (int index = 0; index < allMass.length; index++) {
        allMass[index] = Float.NEGATIVE_INFINITY;
      }
      
      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          for (int index = 0; index < sourceIds.size(); index++) {
            allMass[index] = sumLogProbs(allMass[index], 
                n.getPersonalizedPageRank(sourceIds.get(index)));
          }
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        
        for (int index = 0; index < sourceIds.size(); index++) {
          intermediateMass.setPersonalizedPageRank(sourceIds.get(index), 
              allMass[index]);
        }

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends
      Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost
    // through dangling nodes.
    
    private float[] totalMass, allMass;
    private ArrayListOfIntsWritable sourceIds = new ArrayListOfIntsWritable();
    private static final ArrayOfFloatsWritable MASS_TO_WRITE = new ArrayOfFloatsWritable();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      
      String[] temp = conf.get("SourceNodes").split(",");
      for (int i = 0; i < temp.length; i++) {
        int nid = Integer.parseInt(temp[i]);
        sourceIds.add(nid);
      }
      
      totalMass = new float[sourceIds.size()];
      allMass = new float[sourceIds.size()];
      for (int index = 0; index < totalMass.length; index++) {
        totalMass[index] = Float.NEGATIVE_INFINITY;
      }
      
    }
    
    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context)
        throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      // Create the node structure that we're going to assemble back together from shuffled pieces.
      PageRankNode node = new PageRankNode();
      node.setSourceIds(sourceIds);

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;
      
      for (int index = 0; index < allMass.length; index++) {
        allMass[index] = Float.NEGATIVE_INFINITY;
      }
      
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacencyList();
          structureReceived++;

          node.setAdjacencyList(list);
        } else {
          
          // This is a message that contains PageRank mass; accumulate.
          for (int index = 0; index < sourceIds.size(); index++) {
            allMass[index] = (sumLogProbs(allMass[index],
                n.getPersonalizedPageRank(sourceIds.get(index))));
          }
          
          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      for (int index = 0; index < sourceIds.size(); index++) {
        node.setPersonalizedPageRank(sourceIds.get(index), allMass[index]);
      }
      
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        //System.out.println(node.getPagerankValues());
        
        context.write(nid, node);
        
        // Keep track of total PageRank mass.
        for (int index = 0; index < sourceIds.size(); index++) {
          
          totalMass[index] = sumLogProbs(totalMass[index], allMass[index]);
          
        }
        
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no
        // corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: "
            + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass
        // was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get()
            + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      
      System.out.println("totalMass from phase1: " + totalMass);
      
      MASS_TO_WRITE.setArray(totalMass);
      MASS_TO_WRITE.write(out);
      //totalMass.write(out);
      out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care
  // of the random jump factor.
  private static class MapPageRankMassDistributionClass extends
      Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    
    private float[] missingMass;
    private IntWritable keyTemp = new IntWritable();
    private ArrayListOfIntsWritable sourceIds = new ArrayListOfIntsWritable();

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      missingMass = readArrayFloatsFromString(conf.get("MissingMass"));
      System.out.println("conf: " + conf.get("MissingMass"));
      System.out.println(missingMass);
      
      String[] temp = conf.get("SourceNodes").split(",");
      for (int i = 0; i < temp.length; i++) {
        sourceIds.add(Integer.parseInt(temp[i]));
      }
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context)
        throws IOException, InterruptedException {
      
      // in personalized page rank, instead of distributing the missingMass
      // to all nodes, it just goes back to the source.
      
      for (int index = 0; index < sourceIds.size(); index++) {
        int sid = sourceIds.get(index); 
        if (sid == node.getNodeId()) {
          
          float p = node.getPersonalizedPageRank(sid);
          keyTemp.set(sid);
          
          float jump = (float) Math.log(ALPHA);
          float self = (float) Math.log(1.0f - ALPHA) + 
              sumLogProbs(p, (float) Math.log(missingMass[index]));
          
          //p = sumLogProbs(p, (float) Math.log(missingMass.get(keyTemp).get()));
          p = sumLogProbs(jump, self);
          
          node.setPersonalizedPageRank(sid, p);
          
        } else {
          float p = node.getPersonalizedPageRank(sid);
          p = p + (float)Math.log(1.0f - ALPHA);
          node.setPersonalizedPageRank(sid, p);
        }
      }
      
      context.write(nid, node);
    }
    
    private float[] readArrayFloatsFromString(String strInput) {
      // example: [0.05,2.56,9.876,]
      
      // removing the angle braces
      String temp = strInput.substring(1, strInput.length()-1);
      
      // get the key=value with some spaces at the beginning
      String[] elems = temp.split(",");
      
      ArrayList<Float> tempStore = new ArrayList<Float>();
      
      for (String e : elems) {
        if (!e.isEmpty()) {
          tempStore.add(Float.parseFloat(e));
        }
      }
      
      float[] missingMass = new float[tempStore.size()];
      for (int index = 0; index < tempStore.size(); index++) {
        missingMass[index] = tempStore.get(index);
      }
      
      return missingMass;
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

	/**
	 * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
	 */
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
	}

	public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String COMBINER = "useCombiner";
  private static final String INMAPPER_COMBINER = "useInMapperCombiner";
  private static final String RANGE = "range";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(new Option(COMBINER, "use combiner"));
    options.addOption(new Option(INMAPPER_COMBINER, "user in-mapper combiner"));
    options.addOption(new Option(RANGE, "use range partitioner"));

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg()
        .withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg()
        .withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || 
        !cmdline.hasOption(START) ||
        !cmdline.hasOption(END) || 
        !cmdline.hasOption(NUM_NODES) ||
        !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

		String basePath = cmdline.getOptionValue(BASE);
		int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
		int s = Integer.parseInt(cmdline.getOptionValue(START));
		int e = Integer.parseInt(cmdline.getOptionValue(END));
		boolean useCombiner = cmdline.hasOption(COMBINER);
		boolean useInmapCombiner = cmdline.hasOption(INMAPPER_COMBINER);
		boolean useRange = cmdline.hasOption(RANGE);
		
		String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: RunPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - source nodes: " + sources);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - use combiner: " + useCombiner);
    LOG.info(" - use in-mapper combiner: " + useInmapCombiner);
    LOG.info(" - user range partitioner: " + useRange);

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, useCombiner, useInmapCombiner, sources);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int i, int j, String basePath, int numNodes,
      boolean useCombiner, boolean useInMapperCombiner, String sources) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    float[] mass = 
        phase1(i, j, basePath, numNodes, useCombiner, useInMapperCombiner, sources);

    // Find out how much PageRank mass got lost at the dangling nodes.
    float[] missing = new float[mass.length];
    
    for (int index = 0; index < mass.length; index++) {
      missing[index] = 1.0f - (float) StrictMath.exp(mass[index]);
    }
    
    //float missing = 1.0f - (float) StrictMath.exp(mass);

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(i, j, missing, basePath, numNodes, sources);
  }

  private float[] phase1(int i, int j, String basePath, int numNodes,
      boolean useCombiner, boolean useInMapperCombiner, String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(i);
    String out = basePath + "/iter" + formatter.format(j) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because
    // the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + j + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info(" - useCombiner: " + useCombiner);
    LOG.info(" - useInmapCombiner: " + useInMapperCombiner);
    LOG.info("computed number of partitions: " + numPartitions);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);
    job.getConfiguration().set("SourceNodes", sources);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    //job.setMapperClass(useInMapperCombiner ? MapWithInMapperCombiningClass.class : MapClass.class);
    job.setMapperClass(MapClass.class);

    if (useCombiner) {
      job.setCombinerClass(CombineClass.class);
    }

    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    ArrayOfFloatsWritable massToAdd = new ArrayOfFloatsWritable(),
        massStored = new ArrayOfFloatsWritable();
    
    FileSystem fs = FileSystem.get(getConf());
    boolean firstTime = true;
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      
      massStored.readFields(fin);
      
      if (firstTime) { 
        massToAdd.setArray(new float[massStored.size()]);
        for (int index = 0; index < massToAdd.size(); index++) {
          massToAdd.set(index, Float.NEGATIVE_INFINITY);
        }
        firstTime = false;
      }
      
      for (int index = 0; index < massToAdd.size(); index++) {
        massToAdd.set(index, sumLogProbs(massToAdd.get(index), massStored.get(index)));
      }
      
      fin.close();
    }

    return massToAdd.getArray();
  }

  private void phase2(int i, int j, float[] missing, 
      String basePath, int numNodes, String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + j + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    LOG.info("missing PageRank mass: " + missing);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(j) + "t";
    String out = basePath + "/iter" + formatter.format(j);

    LOG.info("PageRank: iteration " + j + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().set("MissingMass", (new ArrayOfFloatsWritable(missing)).toString());
    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().set("SourceNodes", sources);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
