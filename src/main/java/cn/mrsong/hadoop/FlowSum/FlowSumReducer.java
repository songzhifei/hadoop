package cn.mrsong.hadoop.FlowSum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
	public long up_flow = 0;
	public long down_flow = 0;
	@Override
	protected void reduce(Text key, Iterable<FlowBean> value, Context context)
			throws IOException, InterruptedException {
		
		for (FlowBean flowBean : value) {
			this.up_flow += flowBean.getUp_flow();
			this.down_flow += flowBean.getDown_flow();
		}
		context.write(key, new FlowBean(key.toString(),this.up_flow,this.down_flow));
	}

}
