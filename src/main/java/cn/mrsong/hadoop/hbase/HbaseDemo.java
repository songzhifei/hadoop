package cn.mrsong.hadoop.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class HbaseDemo {

	public static void main(String[] args) throws Exception, ZooKeeperConnectionException, IOException {

		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.property.clientPort","2181");
		conf.set("hbase.zookeeper.quorum","192.168.112.133:2181,192.168.1.22:2181,192.168.112.134:2181");
		conf.set("hbase.master", "http://192.168.112.133:60010");
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (!admin.tableExists("test")) {
		    HTableDescriptor tableDesc=new HTableDescriptor(TableName.valueOf("test"));
		    HColumnDescriptor cf=new HColumnDescriptor("cf");
		    tableDesc.addFamily(cf);
		    admin.createTable(tableDesc);
		}
		//admin.createTable(arg0, arg1);
	}

}
