import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import java.util.*;

public class LatestAccessReducer extends Reducer<KeyPair, Text, Text, Text> {
	public void reduce(KeyPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		Iterator<Text> it = values.iterator();

		for(int count=0;count<3;count++) {
			if (it.hasNext()) {
				context.write(key.getIpAddress(), new Text("\t["+values.iterator().next()+"]"));
			}
		}
	}
}
