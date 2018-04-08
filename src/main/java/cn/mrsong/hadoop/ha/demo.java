package cn.mrsong.hadoop.ha;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
//import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class demo {

	public static void main(String[] args) throws IOException, Exception {
		Configuration conf = new Configuration();
		conf.set("dfs.nameservices", "ns1");
		conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
		//conf.set("dfs.namenode.rpc-address.ns1.nn1", "itcast03:9000");
		//conf.set("dfs.namenode.rpc-address.ns1.nn2", "itcast04:9000");
		conf.set("dfs.namenode.rpc-address.ns1.nn1", "itcast02:9000");
		conf.set("dfs.namenode.rpc-address.ns1.nn2", "itcast03:9000");
		conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
		FileSystem fs= FileSystem.get(new URI("hdfs://ns1"), conf,"root");
		//上传文件
		//FileInputStream inputStream = new FileInputStream("C://Users/Administrator/Desktop/words.txt");
		//FSDataOutputStream outputStream = fs.create(new Path("/words.txt")); 
		//下载文件
		InputStream inputStream= fs.open(new Path("/words.txt"));
		
		FileOutputStream outputStream = new FileOutputStream("d://words.txt");
		
		IOUtils.copyBytes(inputStream, outputStream, 4096, true);

	}

}
