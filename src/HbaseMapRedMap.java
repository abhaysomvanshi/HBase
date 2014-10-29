
import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class HbaseMapRedMap extends TableMapper<Text, IntWritable> {
	 
	/*public static final byte[] CF = "cf1".getBytes();
	public static final byte[] ATTR1 = "attr1".getBytes();

	private final IntWritable ONE = new IntWritable(1);
   	private Text text = new Text();
   	//text="abhay";
   	
   	public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {
    	String val = new String(value.getValue(CF, ATTR1));
      	//text.set("abhay");     // we can only emit Writables...

    	context.write(text, ONE);
	}*/
	
	@Override
	 public void map(ImmutableBytesWritable rowKey, Result columns, Context context)
	   throws IOException, InterruptedException {

	  try {
	   // get rowKey and convert it to string
	   String inKey = new String(rowKey.get());
	   // set new key having only date
	   String storeKey = inKey.split("#")[0];
	   // get sales column in byte format first and then convert it to string (as it is stored as string from hbase shell)
	   byte[] bSales = columns.getValue(Bytes.toBytes("cfSales"), Bytes.toBytes("Sales"));
	   
	   String AggregateSales = new String(bSales);
	   Integer sales = new Integer(AggregateSales);
	   // emit date and sales values
	   context.write(new Text(storeKey), new IntWritable(sales));
	  } catch (RuntimeException e){
	   e.printStackTrace();
	  }
	 }
}
	