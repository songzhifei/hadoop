package cn.mrsong.hadoop.FlowSum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class FlowBean implements Writable  {
	public String phoneNB;
    private long up_flow;  
    private long down_flow;  
    private long sum_flow;
    
	public FlowBean() {

	}
    // 为了对象数据的初始化方便，加入一个带参的构造函数  
    public FlowBean(String phoneNB, long up_flow, long down_flow) {  
        this.phoneNB = phoneNB;  
        this.up_flow = up_flow;  
        this.down_flow = down_flow;  
        this.sum_flow = up_flow + down_flow;  
    }  
	@Override
	public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNB);  
        out.writeLong(up_flow);  
        out.writeLong(down_flow);  
        out.writeLong(sum_flow);  
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.phoneNB = in.readUTF();
		this.up_flow = in.readLong();
		this.down_flow = in.readLong();
		this.sum_flow = this.up_flow + this.down_flow;
	}
	
    public void setUp_flow(long up_flow) {  
        this.up_flow = up_flow;  
    }  
    public long getUp_flow() {  
        return this.up_flow;  
    }  
    public long getDown_flow() {  
        return down_flow;  
    }  
  
    public void setDown_flow(long down_flow) {  
        this.down_flow = down_flow;  
    }  
  
    public long getSum_flow() {  
        return sum_flow;  
    }  
  
    public void setSum_flow(long sum_flow) {  
        this.sum_flow = sum_flow;  
    }  

	@Override
	public String toString() {
		return "" + up_flow + "\t" + down_flow + "\t" + sum_flow;
	}

}
