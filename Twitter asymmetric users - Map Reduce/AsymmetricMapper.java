import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class AsymmetricMapper extends Mapper<LongWritable, Text,  Text, IntWritable> {
  
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
   	String[] parts = line.split("\\s+");
   	String user_id = parts[0];
   	String follower = parts[1];
   	
   	context.write(new Text(user_id+"\t"+follower),new IntWritable(1));
   	
   	context.write(new Text(follower+"\t"+user_id), new IntWritable(-1));
  }
}
  

