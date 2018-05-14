package cn.mrsong.hadoop20180514;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ActorPartitioner extends Partitioner<Text, Text> {

	@Override
	public int getPartition(Text key, Text value, int numReduceTasks) {
		String sex = key.toString();           
		if(numReduceTasks==0)
			return 0;
		//性别为male 选择分区0
		if(sex.equals("male"))             
			return 0;
		//性别为female 选择分区1
		if(sex.equals("female"))
			return 1 % numReduceTasks;
		//其他性别 选择分区2
		else
			return 2 % numReduceTasks;
	}
	
}
