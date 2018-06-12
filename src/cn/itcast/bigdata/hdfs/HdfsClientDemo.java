package cn.itcast.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
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
        /**
         * 参数优先级： 1、客户端代码中设置的值 2、classpath下的用户自定义配置文件 3、然后是服务器的默认配置
         */
        conf.set("dfs.replication", "3");

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
        Path dst = new Path("/hdfs/java");
        fs.copyFromLocalFile(src, dst);
        fs.close();
    }

    /**
     * 从hdfs下载文件
     *
     * @throws Exception
     */
    @Test
    public void testDownloadFileToLocal() throws IOException {
        Path src = new Path("/hdfs/java");
        Path dst = new Path("C:/Users/admin/Desktop/python/hive2.sql");
        fs.copyToLocalFile(src, dst);
        fs.close();
    }

    @Test
    public void testMkdirAndDeleteAndRename() throws IOException {
        //创建文件夹
        fs.mkdirs(new Path("/mkdir"));
        fs.mkdirs(new Path("/mkdir2/aaa/bbb"));

        //删除文件
        fs.delete(new Path("/mkdir"), false);
        //删除非空的文件要加为true
        fs.delete(new Path("/aaa"), true);
        fs.delete(new Path("/mkdir2"), true);

        //重命名
        fs.rename(new Path("/hdfs/java"), new Path("/hdfs/java2"));
        fs.close();
    }

    /**
     * 查看目录信息，只显示文件
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testListFiles() throws IOException {
        //为什么返回Iterator,不是集合和数组  list太大对客户端内存压力大,迭代器包含指针小巧
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件路径:" + fileStatus.getPath().getName());
            System.out.println("文件块大小:" + fileStatus.getBlockSize());
            System.out.println("文件权限:" + fileStatus.getPermission());
            System.out.println("文件大小:" + fileStatus.getLen());
            BlockLocation[] locations = fileStatus.getBlockLocations();
            for (BlockLocation location : locations) {
                System.out.println("block-length:{}" + location.getLength() + "---" + "block-offset:{}" + location.getOffset());
                String[] hosts = location.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }

            }
            System.out.println("<----------------文件分割线---------------->");
        }
        fs.close();
    }

    /**
     * 查看文件及文件夹信息
     *
     * @throws IOException
     * @throws IllegalArgumentException
     */
    @Test
    public void testFileAll() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        String flag = "d--             ";
        for (FileStatus fstatus : listStatus) {

            if (fstatus.isFile()) flag = "f--         ";

            System.out.println(flag + fstatus.getPath().getName());
        }
    }
}
