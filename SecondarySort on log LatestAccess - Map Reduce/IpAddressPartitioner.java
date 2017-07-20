
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class IpAddressPartitioner extends Partitioner<KeyPair, Text>{

	@Override
	public int getPartition(KeyPair key, Text value, int numReducers) {
		// TODO Auto-generated method stub
		return Math.abs(key.getIpAddress().hashCode()% numReducers);
	
	}
	

}
