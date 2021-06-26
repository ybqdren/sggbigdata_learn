package com.github.yabdren.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URI;

/**
 * @author Joan Zhao
 * @version 1.0.0
 * @description
 **/
public class HdfsClient03 {
    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException {

        // 1 获取文件配置信息
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop101:8020"), configuration, "hadoop");

        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }
}
