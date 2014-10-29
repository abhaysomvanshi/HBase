
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class EOS {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] s = value.toString().split(",");
			System.out.println(s[0]);
			System.out.println(s[1]);
			output.collect(new Text(s[0].trim()), new IntWritable(Integer.parseInt(s[1].trim())));
			//output.collect(new Text("Abhay"), new IntWritable(3));
			
		}

		
		
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text,IntWritable> output, Reporter reporter) throws IOException {
			Integer count = 0 ;
			String word ;
			word= key.toString();
			while (values.hasNext()) {
				count = count+ values.next().get() ;
				
			}
			output.collect(new Text(key), new IntWritable(count));
		}
	}
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(EOS.class);
		conf.setJobName("eos");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(Map.class);
		//		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path("file:///home/cloudera/workspace/training/Data.txt"));
		FileOutputFormat.setOutputPath(conf, new Path("file:///home/cloudera/workspace/training/out"));

//			FileInputFormat.setInputPaths(conf, new Path("file:///home/cloudera/Desktop/APMSC14_201311020000.txt"));
//				FileInputFormat.setInputPaths(conf, new Path("file:///home/cloudera/Desktop/input/2013*/*/*"));
//				FileOutputFormat.setOutputPath(conf, new Path("file:///home/cloudera/Desktop/output"));


		JobClient.runJob(conf);
	}
}
