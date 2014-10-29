import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;



public class RowCounter {
	static final String Name= "rowcounter";
	static class RowCounterMapper extends TableMapper<ImmutableBytesWritable, Result> {
	/** Counter enumeration to count the actual rows. */
	public static enum Counters {ROWS};
	
	public void map(ImmutableBytesWritable row, Result values, Context context) 
			throws IOException 
			{
		for (KeyValue value: values.list()) 
		{
			if (value.getValue().length > 0) 
				{
					context.getCounter(Counters.ROWS).increment(1);
					break;
				}
			}
	}
	
	}
	
	
	
	public static Job createSubmittableJob(Configuration conf, String[] args)
			throws IOException {
				String tableName = args[0];
				Job job = new Job(conf, Name + "_" + tableName);
				job.setJarByClass(RowCounter.class);
			// Columns are space delimited
			StringBuilder sb = new StringBuilder();
			final int columnoffset = 1;
			for (int i = columnoffset; i < args.length; i++) {
				if (i > columnoffset) {
					sb.append(" ");
				}
					sb.append(args[i]);
			}
			Scan scan = new Scan();
			scan.setFilter(new FirstKeyOnlyFilter());
			if (sb.length() > 0) {
			for (String columnName :sb.toString().split(" ")) {
			String [] fields = columnName.split(":");
			if(fields.length == 1) {
			scan.addFamily(Bytes.toBytes(fields[0]));
			
			} else {
			scan.addColumn(Bytes.toBytes(fields[0]), Bytes.toBytes(fields[1]));
			}
			}
			}
			// Second argument is the table name.
			job.setOutputKeyClass(NullOutputFormat.class);
			TableMapReduceUtil.initTableMapperJob(tableName, scan,
			RowCounterMapper.class, ImmutableBytesWritable.class, Result.class, job);
			job.setNumReduceTasks(0);
			return job;
			}

	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 1) {
		System.err.println("ERROR: Wrong number of parameters: " + args.length);
		System.err.println("Usage: RowCounter <tablename> [<column1> <column2>...]");
		System.exit(-1);
		}
		Job job = createSubmittableJob(conf, otherArgs);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
