import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MostFrequentDestMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] rec = value.toString().split(",");

		String unCar = rec[8];
		String origin = rec[16];
		String dest = rec[17];

		//Handling the Missing values as NA and Header File 
		if (!unCar.equalsIgnoreCase("UniqueCarrier")
				&& !dest.equalsIgnoreCase("Dest")
				&& !origin.equalsIgnoreCase("Origin")){
			
			if (!unCar.equalsIgnoreCase("NA")&&!dest.equalsIgnoreCase("NA")
				&& !origin.equalsIgnoreCase("NA")) {
				try {
					//  key is 'unique carrier + origin' and value is destination_code
					context.write(new Text(unCar + "\t" + origin), new Text(
							dest));
				} catch (Exception ne) {
				}
			}
		}
	}
}
 
