package com.imooc.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * Hadoop HDFS JAVA api操作
 */
public class HDFSApp {

    public static final String HDFS_PATH = "hdfs://192.168.2.106:8020";

    FileSystem fileSystem = null;
    Configuration configuration = null;

    /**
     * 创建HDFS目录
     * @throws Exception
     */
    @Test
    public void mkdir() throws Exception {
        fileSystem.mkdirs(new Path("/hdfsapi/test"));
    }

    /**
     * 创建文件
     */
    @Test
    public void create() throws Exception {
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/a.txt"));
        fsDataOutputStream.write("hello hadoop".getBytes());
        fsDataOutputStream.flush();
        fsDataOutputStream.close();
    }

    /**
     * 查看HDFS文件的内容
     */
    @Test
    public void cat() throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/hdfsapi/test/a.txt"));
        IOUtils.copyBytes(in, System.out, 1024);
        in.close();

    }


    /**
     * 重命名
     */
    @Test
    public void rename() throws IOException {
        Path oldPath = new Path("/hdfsapi/test/a.txt");
        Path newPath = new Path("/hdfsapi/test/b.txt");
        fileSystem.rename(oldPath, newPath);
    }

    /**
     * 文件上传到HDFS
     */
    @Test
    public void copyFromLocalFile() throws Exception{
        Path localPath = new Path("/Users/hdw/Desktop/land.zip");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);
    }

    /**
     * 文件上传到HDFS（进度条版）
     */
    @Test
    public void copyFromLocalFileWithProgress() throws Exception{
        Path localPath = new Path("/Users/hdw/Desktop/Transform/jdk-8u181-linux-x64.tar.gz");
        Path hdfsPath = new Path("/hdfsapi/test");
        fileSystem.copyFromLocalFile(localPath, hdfsPath);

        InputStream in = new BufferedInputStream(
                new FileInputStream(
                        new File("/Users/hdw/Desktop/Transform/jdk-8u181-linux-x64.tar.gz")));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/hdfsapi/test/jdk-8u181-linux-x64.tar.gz"), new Progressable() {
            public void progress() {
                System.out.print(".");//带进度提醒信息
            }
        });

        IOUtils.copyBytes(in,fsDataOutputStream,4096);
    }

    /**
     * 下载文件到本地
     */
    @Test
    public void copyToLocalFile()throws Exception {
        Path localPath = new Path("/Users/hdw/Desktop");
        Path hdfsPath = new Path("/hdfsapi/test/jdk-8u181-linux-x64.tar.gz");
        fileSystem.copyToLocalFile(hdfsPath,localPath);
    }

    /**
     * 查看某个目录下的所有文件
     */
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
            short replication = fileStatus.getReplication();//副本
            long len = fileStatus.getLen();//文件大小
            String path = fileStatus.getPath().toString();
            System.out.println("isDir:" + isDir + "\t" + "replication:" + replication + "\t" + "len:" + len + "\t\t" + "path" + path);
        }
    }


    /**
     * 删除
     */
    @Test
    public void delete() throws Exception {
        fileSystem.delete(new Path("/hdfsapi/test/"), true);//递归删除
    }



    @Before
    public void setUp() throws Exception {
        System.out.println("HDFSApp.setUp");
        configuration = new Configuration();
        fileSystem = FileSystem.get(new URI(HDFS_PATH),configuration,"root");
    }

    @After
    public void tearDown()throws Exception {
        configuration = null;
        fileSystem = null;
        System.out.println("HDFSApp.tearDown");
    }

}
