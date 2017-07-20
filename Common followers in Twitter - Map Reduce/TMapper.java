import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class TMapper extends Mapper<LongWritable, Text,  Text, Text> {
  
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String line = value.toString();
    String[] parts = line.split("\\s+");
   	String user_id = parts[0];
   	String follower = parts[1];
   	context.write(new Text(user_id), new Text(follower));
  }
}
  

