
import java.io.IOException;
import java.text.ParseException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LatestAccessMapper 
	extends Mapper<LongWritable, Text, KeyPair, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
   
	  try{
		  String record = value.toString();
		  int endIndex = record.indexOf(" ");
		  String ipAdrr = record.substring(0,endIndex);
		  int dateStart =  record.indexOf("[")+1;
		  int dateEnd =record.indexOf("]");
		  String dateTime=record.substring(dateStart,dateEnd);
     context.write(new KeyPair(ipAdrr,dateTime), new Text(dateTime));
  
    }
	  catch(ParseException pe)
	  {
		  
	  }
    }
}
