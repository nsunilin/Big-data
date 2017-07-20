import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CommonFollowersDriver extends Configured implements Tool{
 
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new CommonFollowersDriver(),args);
	    System.exit(exitCode);
  }

	public int run(String[] args) throws Exception {
		 if (args.length != 2) {
		      System.err.println("Usage: CommonFollowers <input path> <output path>");
		      System.exit(-1);
		    }
		    @SuppressWarnings("deprecation")
			Job job1 = new Job(getConf());
		    job1.setJarByClass(CommonFollowersDriver.class);
		    job1.setJobName("Step1");
		    //Create a temporary file to store the result of job1
		    FileInputFormat.addInputPath(job1, new Path(args[0]));
		    Path tempOut = new Path("temp");
		    SequenceFileOutputFormat.setOutputPath(job1, tempOut);
		    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		    job1.setMapperClass(TMapper.class);
		    job1.setReducerClass(TReducer.class);
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);
		    job1.waitForCompletion(true);

		    //Job2 is the mapreduce job for the second step 
		    @SuppressWarnings("deprecation")
			Job job2 = new Job();
		    job2.setJarByClass(CommonFollowersDriver.class);
		    job2.setJobName("Step2");

		    //The input of job2 is the output of job 1
		    job2.setInputFormatClass(SequenceFileInputFormat.class);
		    SequenceFileInputFormat.addInputPath(job2, tempOut);
		    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		    job2.setReducerClass(T2Reducer.class);
		    job2.setMapOutputKeyClass(Text.class);
		    job2.setMapOutputValueClass(Text.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(Text.class);
		    job2.waitForCompletion(true);
		    return(job2.waitForCompletion(true) ? 0 : 1);
	}
}
