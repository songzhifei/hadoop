package cn.mrsong.hadoop.reverse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class reverse {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(reverse.class);
		
		job.setMapperClass(reverseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setCombinerClass(reverseCombinder.class);
		
		job.setReducerClass(reverseReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);

	}
	public static class reverseMapper extends Mapper<LongWritable, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(" ");
			//获取文件名
			FileSplit split = (FileSplit)context.getInputSplit();
			Path path = split.getPath();
			String name = path.getName();
			for(String field : fields){
				k.set(field+"->"+name);
				v.set("1");
				context.write(k, v);
			}
		}
		
		
	}
	public static class reverseCombinder extends Reducer<Text, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String[] keys = key.toString().split("->");
			long sum = 0;
			for(Text text :value){
				sum +=Long.parseLong(text.toString());
			}
			k.set(keys[0]);
			v.set(keys[1]+"->"+sum);
			context.write(k, v);
		}
		
	}
	public static class reverseReducer extends Reducer<Text, Text, Text, Text>{
		Text k = new Text();
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			String val = "";
			for(Text text :value){
				val += text +" ";
			}
			k.set(key);
			v.set(val);
			context.write(key, v);
		}
		
	}

}
