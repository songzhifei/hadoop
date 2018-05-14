package cn.mrsong.hadoop20180514;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActorReducer extends Reducer<Text, Text, Text, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int maxHotIndex = Integer.MIN_VALUE;
		String name = " ";
		int hotIndex = 0;
		for (Text val : values) {
			String[] valTokens = val.toString().split("\\t");
			hotIndex = Integer.parseInt(valTokens[1]);
			if (hotIndex > maxHotIndex) {
				name = valTokens[0];
				maxHotIndex = hotIndex;
			}
		}
		context.write(new Text(name), new Text( key + "\t"+ maxHotIndex));
	}
	
}
