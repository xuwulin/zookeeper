package com.xwl.zookeeper;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author xwl
 * @date 2019-08-26 19:51
 * @description 需求：客户端能实时洞察到服务器上线下变化
 * 模拟服务器注册到zookeeper集群
 */
public class DistributeServer {

    // 日志
    private static final Logger log = LoggerFactory.getLogger(DistributeServer.class);

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
        /**
         * 参数：
         *    connectString：连接服务器的ip字符串
         *      比如："192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181"
         *      可以使一个ip,也可以是多个ip,一个ip代表单机，多个ip代表集群
         *    sessionTimeout：超时时间，心跳收不到了，那就超时了
         *    watcher：通知时间，如果有对应的时间触发，则会收到一个通知；如果不需要，那就设置为null
         *    canBeReadOnly：可读，当这个物理机节点断开后，还是可以读到数据的，只是不能写。此时数据被读取到的可能是旧数据，此处建议设置为false，不推荐使用
         *    sessionId：会话的id
         *    sessionPasswd：会话密码，当会话丢失后，可依据sessionId和sessionPasswd重新获取会话
         *
         */
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                log.info("接收到watch通知：{}", watchedEvent);
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
