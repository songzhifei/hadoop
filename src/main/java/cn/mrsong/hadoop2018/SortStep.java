package cn.mrsong.hadoop2018;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import cn.mrsong.hadoop2018.SumStep.SumMapper;
import cn.mrsong.hadoop2018.SumStep.SumReducer;


public class SortStep {

	public static void main(String[] args) throws Exception {
		
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(SumStep.class);
		job.setMapperClass(SumMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(InfoBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
		Job job2 = Job.getInstance(conf);
		job2.setJarByClass(SortStep.class);
		job2.setMapperClass(SortMapper.class);
		job2.setMapOutputKeyClass(InfoBean.class);
		job2.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job2, new Path(args[1]));
		
		job2.setReducerClass(SortReducer.class);
		job2.setOutputKeyClass(InfoBean.class);
		job2.setMapOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		ControlledJob aJob = new ControlledJob(job.getConfiguration());
		ControlledJob bJob = new ControlledJob(job2.getConfiguration());
		
		aJob.setJob(job);
		bJob.setJob(job2);
		
		//指定依赖关系
		bJob.addDependingJob(aJob);
		
		JobControl jc = new JobControl("排序");
		
		jc.addJob(aJob);
		jc.addJob(bJob);
		
		Thread thread = new Thread(jc);
		
		thread.start();
		
		while (!jc.allFinished()) {
			Thread.sleep(500);
		}
		
		jc.stop();
		//job2.waitForCompletion(true);

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
