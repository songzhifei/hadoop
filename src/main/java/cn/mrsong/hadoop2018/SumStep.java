package cn.mrsong.hadoop2018;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SumStep {

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
		
		job.waitForCompletion(true);
		

	}
	public static class SumMapper extends Mapper<LongWritable, Text,Text,InfoBean>{
		Text k = new Text();
		InfoBean v = new InfoBean();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] fileds = line.split("\t");
			String account = fileds[0];
			double in = Double.parseDouble(fileds[1]);
			double out = Double.parseDouble(fileds[2]);
			k.set(account);
			v.set(account, in, out);
			context.write(k, v);
		}
		
		
	}
	public  static class SumReducer extends Reducer<Text, InfoBean, Text, InfoBean>{
		InfoBean ib = new InfoBean();
		@Override
		protected void reduce(Text k2, Iterable<InfoBean> v2, Context context)
				throws IOException, InterruptedException {
			double in_sum = 0;
			double out_sum = 0;
			for(InfoBean bean:v2) {
				in_sum += bean.getIncome();
				out_sum += bean.getExpenses();
			}
			ib.set("",in_sum,out_sum);
			context.write(k2, ib);
		}
		
	}
}

