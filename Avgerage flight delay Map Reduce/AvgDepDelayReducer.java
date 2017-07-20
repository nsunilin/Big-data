
import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AvgDepDelayReducer extends
		Reducer<Text, AvgDepDelayPair, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<AvgDepDelayPair> values,
			Context context) throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		// Adding the partial sums and partial counts
		for (AvgDepDelayPair value : values) {
			sum += value.getPartialSum();
			count += value.getPartialCount();
		}
		context.write(key, new DoubleWritable(sum / count));
	}

}
