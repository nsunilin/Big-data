import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TReducer extends 
Reducer<Text, Text, Text,Text>{

  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	
	  ArrayList<Integer> fir=new ArrayList<Integer>();
	  
	  Map<String,Integer> map=new HashMap<String,Integer>();
	  
	  for(Text v:values)
	  {
		  fir.add(Integer.parseInt(v.toString()));
	  }
	  
	  for(int i=0;i<fir.size();i++){
		  Integer r=fir.get(i);
		  for(int j=0;j<fir.size();j++){
			  Integer e=fir.get(j);
			  if(r>e){
				  String s=String.valueOf(e+"\t"+r);
				  if(!map.containsKey(s)){
					  map.put(s,1);
				 // System.out.println("j "+e+"\t"+r);
			  }
			  }if(r<e){
				  String rev=String.valueOf(r+"\t"+e);
				  if(!map.containsKey(rev)){
					  map.put(rev,1);
				  //System.out.println("i "+r+"\t"+e);
			  }
		  }
	  }
	  }
	  
	  //Emitting value as key and key as value
			 for(String ke: map.keySet()){
				 context.write(new Text(ke), new Text(key));
			 }
			  
  } 
 }	 
