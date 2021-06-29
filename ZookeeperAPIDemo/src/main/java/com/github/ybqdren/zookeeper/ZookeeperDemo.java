package com.github.ybqdren.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

/**
 * @author Joan Zhao
 * @version 1.0.0
 * @description
 **/
public class ZookeeperDemo {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        String connectionString = "hadoop101:2181,hadoop102:2181,hadoop103:2181";
        int sessionTimeout = 2000;

        // 创建ZooKeeper客户端
        ZooKeeper zkClient = null;
        ZooKeeper zooKeeper = new ZooKeeper(connectionString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 收到事件通知后的回调函数（用户的业务逻辑）
                System.out.println(watchedEvent.getType()+"--"+watchedEvent.getPath());

                // 再次监听
                try {
                    zkClient.getChildren("/",true);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            }
        });


        // 创建子节点
        // 参数1：要创建的节点的路径
        // 参数2：节点数据
        // 参数3：节点权限
        // 参数4：节点的类型
        String nodeCreated = zkClient.create("/hadoop","jinlian".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);

        // 获取节点 并监听节点变化
        List<String> children =  zkClient.getChildren("/",true);

        for(String child : children){
            System.out.println(child);
        }

        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);

        // 判断Znode是否存在
        Stat stat = zkClient.exists("/idea",false);
        System.out.println(stat==null?"not exist":"exist");
    }
}
