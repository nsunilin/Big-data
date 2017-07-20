import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.apache.hadoop.io.*;


public class KeyPair implements WritableComparable<KeyPair>{
 
	//the key pair holds the ipAddress and log time
	private Text ipAddress;
	private Date dateTime;
	
	//The default constructor
	public KeyPair()
	{
		ipAddress = new Text();
		dateTime= new Date();
	}
	
	//constructor, initializing the ip address and log time
	public KeyPair(String ipadr, String dtTime) throws ParseException
	{
		ipAddress= new Text(ipadr);
		DateFormat dateFormat =  new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z",Locale.ENGLISH);
		dateTime= dateFormat.parse(dtTime);
	}
	 
	
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		ipAddress.readFields(in);
		Text dttime=new Text(dateTime.toString());
		dttime.readFields(in);
	}

	
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		ipAddress.write(out);
		Text dttime=new Text(dateTime.toString());
		dttime.write(out);
	}


	public int compareTo(KeyPair otherPair) {
		// TODO Auto-generated method stub
		int c= ipAddress.compareTo(otherPair.ipAddress);
		if (c!=0)
			return c;
		else
			return dateTime.compareTo(otherPair.dateTime);
	}
	
	//the Getter and setter methods
	public Text getIpAddress() {
		return ipAddress;
	}

	public void setIpAddress(Text ipAdr) {
		this.ipAddress = ipAdr;
	}

	public Date getDateTime() {
		return dateTime;
	}

	public void setDateTime(Date dtTime) {
		this.dateTime = dtTime;
	}

}
