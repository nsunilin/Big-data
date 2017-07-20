

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
public class AvgDepDelayMapper extends
Mapper<LongWritable, Text, Text, AvgDepDelayPair> {
public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
{
		String[] rec = value.toString().split(",");
		String unCar = rec[8];

		try {
			String delay = rec[15];
			context.write(new Text(unCar),
					new AvgDepDelayPair(Double.parseDouble(delay), 1));
		} catch (NumberFormatException ne) {
		}
	}
}