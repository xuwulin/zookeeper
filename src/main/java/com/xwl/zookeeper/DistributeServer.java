package com.xwl.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.List;

/**
 * @author xwl
 * @date 2019-08-26 19:51
 * @description 需求：客户端能实时洞察到服务器上线下变化
 * 模拟服务器注册到zookeeper集群
 */
public class DistributeServer {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeServer server = new DistributeServer();
        // 1.连接zookeeper集群
        server.getConnect();
        // 2.注册节点
        server.regist(args[0]);
        // 3.业务逻辑处理
        server.business();
    }

    // zookeeper集群地址，注意：不能有空格，如果要使用主机名称hadoop101:2181,则需要在windows的hosts文件中配置映射
    private String connectString = "192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181";
    // session超时时间：2s
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    /**
     * 建立连接
     * @throws IOException
     */
    private void getConnect() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {

            }
        });
    }

    /**
     * 注册
     */
    private void regist(String hostname) throws KeeperException, InterruptedException {
        String path = zkClient.create("/servers/server", hostname.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostname + "is online");
    }

    /**
     * 业务逻辑处理
     * @throws InterruptedException
     */
    public void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}
