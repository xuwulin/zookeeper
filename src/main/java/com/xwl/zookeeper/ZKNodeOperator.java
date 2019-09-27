package com.xwl.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author xwl
 * @date 2019-09-26 12:02
 * @description zookeeper连接demo
 */
public class ZKNodeOperator implements Watcher {

    private ZooKeeper zooKeeper = null;

    // 日志
    private static final Logger log = LoggerFactory.getLogger(ZKNodeOperator.class);

    // 单机版地址
//    public static final String zkServerPath = "192.168.92.102:2181";
    // zookeeper集群地址，注意：不能有空格，如果要使用主机名称hadoop101:2181,则需要在windows的hosts文件中配置映射
    public static final String zkServerPath = "192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181";

    // session超时时间：2s
    public static final Integer timeout = 5000;

    public ZKNodeOperator() {}

    public ZKNodeOperator(String connectString) {
        try {
            zooKeeper = new ZooKeeper(connectString, timeout, new ZKNodeOperator());
        } catch (IOException e) {
            e.printStackTrace();
            if (zooKeeper != null) {
                try {
                    zooKeeper.close();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建zk节点
     * @param path
     * @param data
     * @param acls
     */
    public void createZKNode(String path, byte[] data, List<ACL> acls) {
        String result = "";
        try {
            /**
             * 同步或者异步创建节点，都不支持子节点的递归创建，异步有一个callback函数
             * 参数：
             * path：创建的路径
             * data：存储的数据的byte[]
             * acl：控制权限策略
             *      Ids.OPEN_ACL_UNSAFE --> world:anyone:cdrwa
             *      CREATOR_ALL_ACL --> auth:user:password:cdrwa
             * createMode：节点类型，是一个枚举
             *      PERSISTENT：持久节点
             *      PERSISTENT_SEQUENTIAL：持久顺序节点
             *      EPHEMERAL：临时节点
             *      EPHEMERAL__SEQUENTIAL：临时顺序节点
             */
            // 同步方式创建
            result = zooKeeper.create(path, data, acls, CreateMode.EPHEMERAL);

            // 异步方式创建
//            String ctx = "{'create':'success'}";
//            zooKeeper.create(path, data, acls, CreateMode.PERSISTENT, new CreateCallBack(), ctx);
//            new Thread().sleep(2000);

            System.out.println("创建节点：\t" + result + "\t成功...");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZKNodeOperator zkServer = new ZKNodeOperator(zkServerPath);

        // 创建zk节点
        zkServer.createZKNode("/testnode", "testnode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);

        /**
         * 修改节点
         * 参数：
         * path：节点路径
         * data：数据
         * version：数据状态
         */
        Stat status = zkServer.getZooKeeper().setData("/testnode", "xyz".getBytes(), 2);
        System.out.println(status.getAversion());

        /**
         * 删除节点
         * 参数：
         * path：路径
         * version：数据状态
         */
        // 同步删除
        zkServer.createZKNode("/test-delete-node", "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        zkServer.getZooKeeper().delete("/testnode", 0);

        // 异步删除节点：常用！！！
        String ctx = "{'delete':'success'}";
        zkServer.getZooKeeper().delete("/test-delete-node", 0, new DeleteCallBack(), ctx);
        Thread.sleep(2000);
    }

    public ZooKeeper getZooKeeper() {
        return zooKeeper;
    }

    public void setZooKeeper(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        log.info("接收到watch通知：{}", watchedEvent);
    }
}
