package cn.mrsong.hadoop.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public class AreaUDF extends UDF {
	private static Map<String, String> areaMap = new HashMap<String, String>();
	
	static {
		areaMap.put("AFG", "北京");
		areaMap.put("NLD", "上海");
	}
	
	public Text evaluate(Text in){
		String result = areaMap.get(in.toString());
		if(result == null){
			result = "其他";
		}
		return new Text(result);
	}
}
