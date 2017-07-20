import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;


public class AvgDepDelayCombiner extends
Reducer<Text,AvgDepDelayPair, Text, AvgDepDelayPair> {
public void reduce(Text key, Iterable<AvgDepDelayPair> values, Context context)
  throws IOException, InterruptedException {
double sum = 0;
int count=0;
for (AvgDepDelayPair value : values) {
  sum += value.getPartialSum();
  count+= value.getPartialCount();
}
context.write(key, new AvgDepDelayPair(sum,count));
} 
}
