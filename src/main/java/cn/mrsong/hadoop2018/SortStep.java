package cn.mrsong.hadoop2018;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SortStep {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortStep.class);
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}
	public static class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable>{

		InfoBean k = new InfoBean();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fileds = line.split("\t");
			String account = fileds[0];
			double in = Double.parseDouble(fileds[1]);
			double out = Double.parseDouble(fileds[2]);

			k.set(account, in, out);
			context.write(k, NullWritable.get());
		}
	}
	public static class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean>{
		Text k = new Text();
		@Override
		protected void reduce(InfoBean bean, Iterable<NullWritable> value,
				Context context) throws IOException, InterruptedException {
			String account = bean.getAccount();
			k.set(account);
			context.write(k,bean);
		}
		
	}
}
