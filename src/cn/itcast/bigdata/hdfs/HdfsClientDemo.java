package cn.itcast.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * @description: hdfsClient对文件 增删改查
 * @author: wsy
 * @date: Created in 14:23 2018/5/31
 * @version: $v: 1.0.0
 */
public class HdfsClientDemo {

    FileSystem fs = null;

    @Before
    public void init() throws Exception {

        //构建一个配置参数对象,设置一个参数,我们要访问的hdfs的Url(core-site.xml)
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://192.168.173.10:9000");

        //获取hdfs的访问客户端,根据参数获取DistributedFileSystem实例(fs.defaultFS)
        //fs = FileSystem.get(conf);
        fs = FileSystem.get(new URI("hdfs://192.168.173.10:9000"), conf, "root");

    }


    /**
     * 往hdfs上传文件
     *
     * @throws Exception
     */
    @Test
    public void testAddFileToHdfs() throws Exception {
        Path src = new Path("C:/Users/admin/Desktop/python/hive1.sql");
        Path dst = new Path("/data/hdfs");
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

}
