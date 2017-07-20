
import org.apache.hadoop.io.*;

public class IpAddressComparator extends WritableComparator{
	
	public IpAddressComparator() {
		super(KeyPair.class, true);
		
	}
	@SuppressWarnings("rawtypes")
	public int compare(WritableComparable k1, WritableComparable k2)
	{
		KeyPair key1 = (KeyPair) k1;
		KeyPair key2= (KeyPair) k2;
		return key1.getIpAddress().compareTo(key2.getIpAddress());
	}
}
