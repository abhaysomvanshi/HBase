import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;


public class HbaseMapRed {
	public static void main(String[] args) throws Exception {
		Configuration config = HBaseConfiguration.create();
		Job job = new Job(config,"ExampleSummary");
		job.setJarByClass(HbaseMapRed.class);     // class that contains mapper and reducer

		Scan scan = new Scan();
		scan.setCaching(500);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		// set other scan attrs

		TableMapReduceUtil.initTableMapperJob(
			"sales",        // input table
			scan,               // Scan instance to control CF and attribute selection
			HbaseMapRedMap.class,     // mapper class
			Text.class,         // mapper output key
			IntWritable.class,  // mapper output value
			job);
		//job.setOutputFormatClass(NullOutputFormat.class);
		TableMapReduceUtil.initTableReducerJob(
			"sales_StoreWise",        // output table
			HbaseMapRedReduce.class,    // reducer class
			job);
		job.setNumReduceTasks(1);   // at least one, adjust as required

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		   
	}
}


