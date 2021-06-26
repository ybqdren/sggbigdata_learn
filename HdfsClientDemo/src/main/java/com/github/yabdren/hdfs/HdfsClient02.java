package com.github.yabdren.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


/**
 * @author Joan Zhao
 * @version 1.0.0
 * @description
 **/
public class HdfsClient02 {
    FileSystem fileSystem = null;

    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void testFilePut() throws IOException {
        fileSystem.copyFromLocalFile(new Path("E:\\图书\\Hibernate3.1.2Chinese.pdf"),new Path("/"));
    }

    /**
     * 下载
     */
    @Test
    public void testFileDown() throws IOException {
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fileSystem.copyToLocalFile(false,
                new Path("/Hibernate3.1.2Chinese.pdf"),
                new Path("e:/testfilehdfs.txt")
        );
    }


    /**
     * 给文件改名字
     * @throws IOException
     */
    @Test
    public void testrname() throws IOException {
        fileSystem.rename(new Path("/Hibernate3.1.2Chinese.pdf"),new Path("/Hibernate3.1.2中文文档.pdf"));
    }

    /**
     * 删除文件
     * @throws IOException
     */
    @Test
    public void testDelete() throws IOException {
        fileSystem.delete(new Path("/Hibernate3.1.2中文文档.pdf"),true);
    }

    @Before
    public void before() throws IOException, InterruptedException {
      fileSystem = FileSystem.get(URI.create("hdfs://hadoop101:8020"),
                new Configuration(),"hadoop");
    }

    @After
    public void after() throws IOException {
        fileSystem.close();
    }
}
