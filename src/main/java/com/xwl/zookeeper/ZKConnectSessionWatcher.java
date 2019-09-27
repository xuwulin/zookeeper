package com.xwl.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author xwl
 * @date 2019-09-26 12:17
 * @description zk会话重连机制
 */
public class ZKConnectSessionWatcher implements Watcher {
    // 日志
    private static final Logger log = LoggerFactory.getLogger(ZKConnectSessionWatcher.class);

    // 单机版地址
//    public static final String zkServerPath = "192.168.92.102:2181";
    // zookeeper集群地址，注意：不能有空格，如果要使用主机名称hadoop101:2181,则需要在windows的hosts文件中配置映射
    public static final String zkServerPath = "192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181";

    // session超时时间：2s
    public static final Integer timeout = 5000;

    public static void main(String[] args) throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper(zkServerPath, timeout, new ZKConnectSessionWatcher());

        long sessionId = zk.getSessionId();
        String ssid = "0x" + Long.toHexString(sessionId);
        System.out.println(ssid);
        byte[] sessionPasswd = zk.getSessionPasswd();

        log.warn("客户端开始连接zookeeper服务器...");
        log.warn("连接状态：{}", zk.getState());
        new Thread().sleep(1000);
        log.warn("连接状态：{}", zk.getState());

        new Thread().sleep(200);

        // 开始会话重连
        log.warn("开始会话重连");

        ZooKeeper zkSession = new ZooKeeper(zkServerPath, timeout, new ZKConnectSessionWatcher(), sessionId, sessionPasswd);

        log.warn("重新连接状态zkSession：{}", zkSession.getState());
        new Thread().sleep(1000);
        log.warn("重新连接状态zkSession：{}", zkSession.getState());

    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watch通知：{}", watchedEvent);
    }
}
