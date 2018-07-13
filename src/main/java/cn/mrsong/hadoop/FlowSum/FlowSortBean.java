package cn.mrsong.hadoop.FlowSum;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class FlowSortBean implements WritableComparable<FlowSortBean>  {
	public String phoneNB;
    private long up_flow;  
    private long down_flow;  
    private long sum_flow;
    
	public FlowSortBean() {

	}
    // 为了对象数据的初始化方便，加入一个带参的构造函数  
    public FlowSortBean(String phoneNB, long up_flow, long down_flow) {  
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
  
    public void setPhoneNB(String phoneNB) {  
        this.phoneNB = phoneNB;  
    }  
    public String getPhoneNB() {  
        return phoneNB;  
    }  
  
    public void setSum_flow(long sum_flow) {  
        this.sum_flow = sum_flow;  
    }  
	@Override
	public String toString() {
		return "" + up_flow + "\t" + down_flow + "\t" + sum_flow;
	}
	@Override
	public int compareTo(FlowSortBean flowBean) {
		
		return this.sum_flow >= flowBean.sum_flow ? 1:-1;
		
	}
}
