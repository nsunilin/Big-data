
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;

public class AvgDepDelayDriver extends Configured implements Tool {
		  public static void main(String[] args) throws Exception {	  
		    	int exitCode = ToolRunner.run(new Configuration(), new AvgDepDelayDriver(),args);
		    System.exit(exitCode);
		  }
		
		  public int run(String[] args) throws Exception {
			  if (args.length != 2) {
			      System.err.println("Usage: Avg Dep Delay <input path> <output path>");
			      System.exit(-1);
			    }
			    //Initializing the map reduce job
			    @SuppressWarnings("deprecation")
				Job job= new Job();		
			    job.setJarByClass(AvgDepDelayDriver.class);
			    job.setJobName("Average Delay");

			    //Setting the input and output paths.The output file should not already exist. 
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));

			    //Setting the mapper, reducer, and combiner classes
			    job.setMapperClass(AvgDepDelayMapper.class);
			    job.setReducerClass(AvgDepDelayReducer.class);
			    job.setCombinerClass(AvgDepDelayCombiner.class);
			    
			    /* 
			     * setting the format of the output key-value pair emitted by the mapper. You need to do this
			     * if the type of the key-value pair produced by the mapper is different than the
			     * type  of the key-value pair the is produced by the reducer 
			     */
			    job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(AvgDepDelayPair.class);
			    
			    //Setting the format of the key-value pair to write in the output file.
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(DoubleWritable.class);

			    //Submit the job and wait for its completion
			    return(job.waitForCompletion(true) ? 0 : 1);
			}
	  
    
  }

