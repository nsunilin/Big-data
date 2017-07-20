import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class T2Reducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context)
  throws IOException, InterruptedException {

		StringBuffer string=new StringBuffer();
		
		for(Text value:values){
			string.append(value);
			string.append(",");
		}
		
		context.write(key, new Text("("+string.toString()+")"));
}
}