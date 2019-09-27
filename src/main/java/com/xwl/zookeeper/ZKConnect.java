package com.xwl.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xwl
 * @date 2019-09-26 12:02
 * @description zookeeper连接demo
 */
public class ZKConnect implements Watcher {

    // 日志
    private static final Logger log = LoggerFactory.getLogger(ZKConnect.class);

    // 单机版地址
//    public static final String zkServerPath = "192.168.92.102:2181";
    // zookeeper集群地址，注意：不能有空格，如果要使用主机名称hadoop101:2181,则需要在windows的hosts文件中配置映射
    public static final String zkServerPath = "192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181";

    // session超时时间：2s
    public static final Integer timeout = 5000;

    public static void main(String[] args) throws IOException, InterruptedException {
        /**
         * 客户端和zk服务端连接是一个异步的过程
         * 当连接成功后，客户端会收到一个watch通知
         *
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
        ZooKeeper zk = new ZooKeeper(zkServerPath, timeout, new ZKConnect());
        log.info("客户端开始连接zookeeper服务器...");
        log.info("连接状态：{}", zk.getState());

        new Thread().sleep(2000);

        log.info("连接状态：{}", zk.getState());

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watch通知：{}", watchedEvent);
    }
}
