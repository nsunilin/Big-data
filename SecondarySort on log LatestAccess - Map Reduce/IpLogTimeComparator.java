
import org.apache.hadoop.io.*;

public class IpLogTimeComparator extends WritableComparator {
	
	public IpLogTimeComparator() {
		super(KeyPair.class, true);	
	}
	
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		int c = key1.getIpAddress().compareTo(key2.getIpAddress());
		if (c ==0)
			//reverse order
			return -key1.getDateTime().compareTo(key2.getDateTime()); 
		else
			return c;
	}
}
