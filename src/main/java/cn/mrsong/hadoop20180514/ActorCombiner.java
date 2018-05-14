package cn.mrsong.hadoop20180514;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ActorCombiner extends Reducer<Text, Text, Text, Text> {
	private Text text = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		int maxHotIndex = Integer.MIN_VALUE;
		int hotIndex = 0;
		String name="";
		for (Text val : values) {
			String[] valTokens = val.toString().split("\\t");
			hotIndex = Integer.parseInt(valTokens[1]);
			if(hotIndex>maxHotIndex){
				name = valTokens[0];
				maxHotIndex = hotIndex;
			}
		}
		text.set(name+"\t"+maxHotIndex);
		context.write(key, text);
	}
	
}
