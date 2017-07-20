import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AsymmetricDriver extends Configured implements Tool{
 
  public static void main(String[] args) throws Exception {	  
    	int exitCode = ToolRunner.run(new Configuration(), new AsymmetricDriver(),args);
    System.exit(exitCode);
  }
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
		      System.err.println("Usage: Join <input path> <output path>");
		      System.exit(-1);
		    }

		    @SuppressWarnings("deprecation")
			Job job = new Job(getConf());
		    job.setJarByClass(AsymmetricDriver.class);
		    job.setJobName("Asymmetric");

		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    job.setMapperClass(AsymmetricMapper.class);
		    job.setReducerClass(AsymmetricReducer.class);
		    //job.setCombinerClass(TwitterJoinReducer.class);
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);

		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(NullWritable.class);

		    return(job.waitForCompletion(true) ? 0 : 1);
		}
}
