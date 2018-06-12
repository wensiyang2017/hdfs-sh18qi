package cn.itcast.bigdata.hdfs.diceng;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 相对那些封装好的方法而言的更底层一些的操作方式
 * 上层那些mapreduce   spark等运算框架，去hdfs中获取数据的时候，就是调的这种底层的api
 *
 * @author
 */
public class DicengAccessDemo {
    FileSystem fs;

    @Before
    public void init() throws Exception {
        Configuration conf = new Configuration();
        fs = FileSystem.get(new URI("hdfs://192.168.173.10:9000"), conf, "root");

    }


    /**
     * 从hdfs下载文件
     *
     * @throws Exception
     */
    @Test
    public void testDownloadFileToLocal() throws IOException {
        //获取一个文件输入流 -- hdfs
        FSDataInputStream in = fs.open(new Path("hdfs://192.168.173.10:9000/data/derby.log"));
        //构造一个文件输出流 -- 本地
        FileOutputStream out = new FileOutputStream("C:/Users/admin/Desktop/python/derby.log");

        //再将输入流
        IOUtils.copyBytes(in,out,10);
    }


}
