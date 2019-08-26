package com.xwl.zookeeper;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xwl
 * @date 2019-08-26 19:51
 * @description 需求：客户端能实时洞察到服务器上线下变化：服务器和客户端对于zookeeper来说都是节点！！！
 * 模拟客户端注册到zookeeper集群
 */
public class DistributeClient {
    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        DistributeClient server = new DistributeClient();
        // 1.连接zookeeper集群
        server.getConnect();
        // 2.注册监听
        server.getChildren();
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
                try {
                    getChildren();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    /**
     * 注册
     */
    private void getChildren() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/servers", true);
        List<String> hosts = new ArrayList<>();
        for (String child : children) {
            byte[] data = zkClient.getData("/servers/" + child, false, null);
            hosts.add(new String(data));
        }
        // 将所有在线主机名称打印到控制台
        System.out.println(hosts);
    }

    /**
     * 业务逻辑处理
     * @throws InterruptedException
     */
    public void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }
}
