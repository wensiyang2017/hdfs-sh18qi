package cn.itcast.bigdata.hdfs;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class ConfigurationReasearch {
	
	public static void main(String[] args) {
		
		Configuration conf = new Configuration();
		
		Iterator<Entry<String, String>> it = conf.iterator();
		
		while(it.hasNext()){
			
			System.out.println(it.next());
			
		}
		
		
		
	}
	

}
