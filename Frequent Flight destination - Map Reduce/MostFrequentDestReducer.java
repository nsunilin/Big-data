
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MostFrequentDestReducer extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
   
   Map<Text,Integer> totalMap=new HashMap<Text,Integer>();
  
   //Counting the destination occurrences 
    for(Text value:values){
    	Integer t=totalMap.get(value);
    	if(t==null){
    		totalMap.put(value, 1); // if first occurrence count =1
    	}else{
    		t+=1;				// otherwise increment the counter 
    		totalMap.put(value,t);
    	}
    }
    
    

   Text temp = new Text();
   Text dest =  new Text();

   Iterator<Text> itm  =  totalMap.keySet().iterator(); // iterating through keys
   
   int maxCol= Collections.max(totalMap.values()); // taking max value of the HashMap
   int frequency = maxCol;
   dest =  itm.next();
  
   while(itm.hasNext())
    {
    	temp=itm.next();
    	int curVal =  totalMap.get(temp);
    	if(curVal==maxCol) // comparing current with max value
    	{
    		frequency = curVal;  // true then set final string as dest and its count 
    		dest = temp;
    	}
    }
    context.write(new Text(key +" "+ dest), new Text(String.valueOf(frequency)));	
  }
}
