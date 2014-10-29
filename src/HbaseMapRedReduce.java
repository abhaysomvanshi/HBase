import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;


public class HbaseMapRedReduce extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
	/*public static final byte[] CF = "cf1".getBytes();
	public static final byte[] COUNT = "count".getBytes();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int i = 0;
		for (IntWritable val : values) {
			i += val.get();
		}
		Put put = new Put(Bytes.toBytes(key.toString()));
		put.add(CF, COUNT, Bytes.toBytes(i));

		context.write(null, put);
	}*/
	 @Override
	 public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	   throws IOException, InterruptedException {
	  try {
	   int sum = 0;
	   // loop through different sales vales and add it to sum
	   for (IntWritable sales : values) {
	    Integer intSales = new Integer(sales.toString());
	    sum += intSales;
	   } 
	   
	   // create hbase put with rowkey as date
	   Put insHBase = new Put(key.getBytes());
	   // insert sum value to hbase 
	   insHBase.add(Bytes.toBytes("cfAggregateSales"), Bytes.toBytes("AggregateSales"), Bytes.toBytes(sum));
	   // write data to Hbase table
	   context.write(null, insHBase);

	  } catch (Exception e) {
	   e.printStackTrace();
	  }
	 }
}	
