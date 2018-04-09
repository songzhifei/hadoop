package cn.mrsong.hadoop.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

//import org.apache.commons.lang.ObjectUtils.Null;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.HColumnDescriptor;
//import org.apache.hadoop.hbase.HTableDescriptor;
//import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
//import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;


public class HbaseDemo {
	
	private  Configuration conf = null;
	
	@Before
	public void init() throws Exception {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum","itcast03:2181,itcast04:2181");
		//conf.set("hbase.zookeeper.quorum","itcast02:2181,itcast03:2181");
		//conf.set("hbase.zookeeper.property.clientPort","2181");
		//conf.set("hbase.master", "http://itcast03:2181:60010");
	}
	@Test
	public void test_put() throws IOException {

		HTable table = new HTable(conf,"people");
		Put put = new Put(Bytes.toBytes("kr0001"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("age"), Bytes.toBytes("3000"));
		put.add(Bytes.toBytes("data"), Bytes.toBytes("fwef"), Bytes.toBytes(3000));
		table.put(put);
		table.close();
	}
	@Test
	public void test_putAll() throws Exception{
		HTable table = new HTable(conf, "people");
		List<Put> puts = new ArrayList<Put>(1000);
		for(int i=1;i<10000;i++) {
			Put put = new Put(Bytes.toBytes("kr0"+i));
			put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(1000+i));
			puts.add(put);
			if(i % 1000 == 0 ) {
				table.put(puts);
				puts = new ArrayList<>(1000);
			}
		}
		table.put(puts);
		table.close();
	}
	@Test
	public void test_get() throws Exception{
		HTable table = new HTable(conf,"people");
		Get get = new Get(Bytes.toBytes("kr0001"));
		Result result = table.get(get);
		table.close();
	  	byte[] bs =result.getValue(Bytes.toBytes("data"), Bytes.toBytes("money"));
	  	System.out.println(Bytes.toInt(bs));
	} 
	@Test
	public void test_getAll() throws Exception{
		HTable table = new HTable(conf,"people");
		List<Get> gets = new ArrayList<Get>();
		for(int i=1;i<100;i++) {
			Get get = new Get(Bytes.toBytes("kr0"+i));
			gets.add(get);
		}
		Result[] results = table.get(gets);
		for (Result result : results) {
			byte[] b = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("money"));
			System.out.println(Bytes.toInt(b));
		}
		table.close();
	} 
	public static void main(String[] args) throws Exception, ZooKeeperConnectionException, IOException {
		/*
		Configuration conf = HBaseConfiguration.create();
		//conf.set("hbase.zookeeper.quorum","itcast03:2181,itcast04:2181");
		conf.set("hbase.zookeeper.quorum","itcast02:2181,itcast03:2181");

		HBaseAdmin admin = new HBaseAdmin(conf);
		if (!admin.tableExists("people")) {
		    HTableDescriptor tableDesc=new HTableDescriptor(TableName.valueOf("people"));
		    HColumnDescriptor info=new HColumnDescriptor("info");
		    info.setMaxVersions(3);
		    HColumnDescriptor data = new HColumnDescriptor("data");
		    tableDesc.addFamily(info);
		    tableDesc.addFamily(data);
		    admin.createTable(tableDesc);
		}
		//关闭admin对象
		admin.close();*/
	}

}
