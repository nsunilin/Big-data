import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class AsymmetricReducer extends 
Reducer<Text, IntWritable, Text,NullWritable>{

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	
	  int tot = 0;
	  
	  for (IntWritable v :  values )
	  {
		//we are summing the values of the key which was emitted by the mapper
		// as +1 if the key is user_id+follwer_id 
		// and -1 if the key is follwer_id+user_id
		  tot += v.get();
	  }
	  
	  //If the sum is zero , it is symmetric and if it's one :it's asymmetric
	  if (tot ==1)
	  {
		  context.write(new Text(key),null);
	  }
  }
	  
}
  
