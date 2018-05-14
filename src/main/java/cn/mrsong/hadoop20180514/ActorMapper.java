package cn.mrsong.hadoop20180514;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ActorMapper extends Mapper<Object, Text, Text, Text> {

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");  
        String gender = tokens[1].trim();//性别  
        String nameHotIndex = tokens[0] + "\t" + tokens[2];//名称和搜索指数  
        context.write(new Text(gender), new Text(nameHotIndex));  
	}
	
}
