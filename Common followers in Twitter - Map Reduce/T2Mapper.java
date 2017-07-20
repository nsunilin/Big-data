import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class T2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String line = value.toString();
    String[] parts = line.split("\\s+");
		String user_id1 = parts[0];
		String user_id2 = parts[1];
		String user_id3 = parts[2];
		context.write(new Text(user_id1+"\t"+user_id2), new Text(user_id3));
}
}