package com.github.yabdren.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @author Joan Zhao
 * @version 1.0.0
 * @description
 **/
public class HdfsClient {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.raplication","2");

        // 1.新建HDFS对象
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop101:8020"),configuration,
                "hadoop");

        // 2. 操作集群
        // 将指定文件上传当根路径
        fileSystem.copyFromLocalFile(new Path("E:\\图书\\Java编程思想（第五版）.pdf"),new Path("/"));

        // 3. 关闭资源
        fileSystem.close();
    }
}
