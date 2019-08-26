package com.xwl.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author xwl
 * @date 2019-08-26 17:17
 * @description 端口说明：
 * 2181是客户端访问集群的时候的端口号
 * 2888是leader和flower之间通讯的端口号
 * 3888是leader和flower之间选举时的端口号
 */
public class TestZookeeper {

    // zookeeper集群地址，注意：不能有空格，如果要使用主机名称hadoop101:2181,则需要在windows的hosts文件中配置映射
    private String connectString = "192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181";
    // session超时时间：2s
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    /**
     * 建立连接
     *
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("-----------start---------------");
                List<String> children = null;
                try {
                    children = zkClient.getChildren("/", true);
                    for (String child : children) {
                        System.out.println(child);
                    }
                    System.out.println("-----------end---------------");
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 1、创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        // 第一个参数：在根目录/下创建 name 节点,
        // 第二个参数：在 name 节点中有 zhangsan
        // 第三个参数：节点权限 ；
        // 第四个参数：节点的类型
        String path = zkClient.create("/name", "zhangsan".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(path);
    }

    /**
     * 2、获取子节点并监听数据变化
     */
    @Test
    public void getChildren() throws KeeperException, InterruptedException {
        // 第一个参数表示：监听根目录下 / 的节点
        // 第二个参数表示：是否开启监控
        List<String> children = zkClient.getChildren("/", true);

        for (String child : children) {
            System.out.println(child);
        }

        // 延时阻塞
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 判断znode是否存在
     */
    @Test
    public void exist() throws Exception {

        Stat stat = zkClient.exists("/eclipse", false);

        System.out.println(stat == null ? "not exist" : "exist");
    }

}
