package cn.mrsong.hadoop.FlowSum;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SortMapReduce {
	public static class FlowSortRunner{
		public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			Configuration configuration = new Configuration();
			Job job = Job.getInstance(configuration);
			
			job.setJarByClass(FlowSortRunner.class);
			
			job.setMapperClass(FlowSortMapper.class);
			job.setReducerClass(FlowSortReducer.class);
			
			job.setMapOutputKeyClass(FlowSortBean.class);
			job.setMapOutputValueClass(NullWritable.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FlowSortBean.class);
			
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			job.waitForCompletion(true);
		}
	}
	public class FlowSortMapper extends Mapper<Writable, Text, FlowSortBean, NullWritable>{

		@Override
		protected void map(Writable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = StringUtils.split(line, "\t");
			String phoneNum = fields[0];
			long up_flow = Long.parseLong(fields[1]);
			long down_flow = Long.parseLong(fields[2]);
			context.write(new FlowSortBean(phoneNum,up_flow,down_flow), NullWritable.get());
		}
		
	}
	public class FlowSortReducer extends Reducer<FlowSortBean, NullWritable, Text, FlowSortBean>{

		@Override
		protected void reduce(FlowSortBean key2, Iterable<NullWritable> value2,Context context)
				throws IOException, InterruptedException {
			String phoneNum = key2.getPhoneNB();
			context.write(new Text(phoneNum), key2);
		}
		
	}
}
