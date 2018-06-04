package cn.itcast.bigdata.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HdfsDemo {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new URI("hdfs://hdp-node01:9000"), conf, "hadoop");
		
		FSDataInputStream in = fs.open(new Path("/jdk-7u65-linux-i586.tar.gz"));
		
		FileOutputStream out = new FileOutputStream(new File("d:/jdk.tgz"));
		
		
		IOUtils.copyBytes(in, out, 4096);
		
		
	}
	

}
