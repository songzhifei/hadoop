package cn.mrsong.hadoop.FlowSum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSumMapper extends Mapper<Writable, Text, Text, FlowBean> {

	@Override
	protected void map(Writable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] lines = value.toString().split("\t");
		String phoneNum =  lines[1];
		long up_flow = Long.parseLong(lines[2]);
		long down_flow = Long.parseLong(lines[3]);
		FlowBean bean = new FlowBean(phoneNum,up_flow,down_flow);
		context.write(new Text(phoneNum), bean);
	}
}
