package com.xwl.zookeeper.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.checkerframework.checker.units.qual.C;

import javax.lang.model.util.ElementScanner6;
import java.security.spec.ECField;
import java.util.List;

/**
 * @author xwl
 * @date 2019-09-26 17:32
 * @description
 */
public class CuratorOperator {
    public CuratorFramework client = null;
    public static final String zkServerPath = "192.168.92.102:2181,192.168.92.103:2181,192.168.92.104:2181";

    /**
     * 实例化zk客户端
     */
    public CuratorOperator() {
        /**
         * 同步创建zk示例，原生api是异步的
         *
         * curator连接zookeeper的策略是：ExponentialBackoffRetry
         * baseSleepTimeMs：初始sleep时间
         * maxRetries：最大重试次数
         * maxSleepMs：最大重试时间
         */
//        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);

        /**
         * curator连接zookeeper的策略：RetryNTimes
         * n:重试次数
         * sleepMsBetweenRetries:每次重试时间间隔
         */
        RetryPolicy retryPolicy = new RetryNTimes(3, 5000);

        /**
         * curator连接zookeeper的策略：RetryOneTime
         * sleepMsBetweenRetries:每次重试时间间隔
         */
//        RetryPolicy retryPolicy = new RetryOneTime(3000);

        /**
         * 永远重试，不推荐使用
         */
//        RetryPolicy retryPolicy = new RetryForever(retryIntervalMs)

        /**
         * curator连接zookeeper的策略：RetryUnitlElapsed
         * maxElapsedTimeMs:最大重试时间
         * sleepMsBetweenRetries：非常重试间隔
         * 重试时间超过maxElapsedTimeMs后，就不再重试
         */
//        RetryPolicy retryPolicy = new RetryUntilElapsed(2000, 3000);

        client = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(10000)
                .retryPolicy(retryPolicy) // 重试机制
                .namespace("workspace") // 在workspace这个工作站（节点）操作
                .build();
        client.start();
    }

    /**
     * 关闭zk客户端连接
     */
    public void closeZkClient() {
        if (client != null) {
            this.client.close();
        }
    }

    public static void main(String[] args) throws Exception {
        // 实例化
        CuratorOperator cto = new CuratorOperator();
        boolean started = cto.client.isStarted();
        System.out.println("当前客户端的状态：" + (started ? "连接中" : "已关闭"));

        // 创建节点
        String nodePath = "/super/imooc";
        byte[] data = "supperme".getBytes();
        cto.client.create().creatingParentsIfNeeded() // 递归创建节点
                .withMode(CreateMode.PERSISTENT) // 创建持久化节点
                .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE) // 默认的ACL权限控制：任何人都可以访问
                .forPath(nodePath, data);

        // 更新节点数据
        byte[] newData = "batman".getBytes();
        cto.client.setData()
                .withVersion(2)
                .forPath(nodePath, newData);

        // 删除节点
        cto.client.delete()
                .guaranteed() // 如果删除失败，那么在后端还是会继续删除，直到成功
                .deletingChildrenIfNeeded() // 如果有子节点，就删除，即递归删除
                .withVersion(2)
                .forPath(nodePath);

        // 读取节点数据
        Stat stat = new Stat();
        byte[] bytes = cto.client.getData().storingStatIn(stat).forPath(nodePath);
        System.out.println("节点" + nodePath + "的数据为：" + new String(bytes));
        System.out.println("该节点的版本号为：" + stat.getVersion());

        // 查询子节点
        List<String> childNodes = cto.client.getChildren().forPath(nodePath);
        System.out.println("开始打印子节点：");
        for (String s : childNodes) {
            System.out.println(s);
        }

        // 判断节点是否存在，如果不存在则为空
        Stat statExist = cto.client.checkExists().forPath(nodePath);
        System.out.println(statExist);

        // watcher事件，当使用usingWatcher的时候，监听只会触发一次，监听完毕后就销毁
        // curator的watcher
        cto.client.getData().usingWatcher(new MyCuratorWatcher()).forPath(nodePath);
        // zk原生的watcher
//        cto.client.getData().usingWatcher(new MyWatcher()).forPath(nodePath);

        // 为节点添加watcher，推荐使用！！！
        // NodeCache：监听数据节点的变更，会触发事件
        final NodeCache nodeCache = new NodeCache(cto.client, nodePath);
        // buildInitial：初始化的时候获取node的值并缓存
        nodeCache.start();
        if (nodeCache.getCurrentData() != null) {
            System.out.println("节点初始化数据为：" + new String(nodeCache.getCurrentData().getData()));
        } else {
            System.out.println("节点初始化数据为空...");
        }
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                String data = new String(nodeCache.getCurrentData().getData());
                System.out.println("节点路径：" + nodeCache.getCurrentData().getPath() + "数据：" + data);
            }
        });

        // 为子节点添加watcher
        // PathChildrenCache：监听数据节点的增删改，会触发事件
        String childNodePathCache = nodePath;
        // cacheData：设置缓存节点的数据状态
        final PathChildrenCache childrenCache = new PathChildrenCache(cto.client, childNodePathCache, true);
        /**
         * StartMode：初始化方式
         * POST_INITIALIZED_EVENT：异步初始化，初始化之后会触发事件
         * NORMAL：异步初始化
         * BUILD_INITIAL_CACHE：同步初始化
         */
        childrenCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
        List<ChildData> childDataList = childrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点数据列表：");
        for (ChildData cd : childDataList) {
            String childData = new String(cd.getData());
            System.out.println(childData);
        }

        childrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event) throws Exception {
                if (event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)) {
                    System.out.println("子节点初始化ok...");
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                    String path = event.getData().getPath();
                    if (path.equals("super/imooc/d")) {
                        System.out.println("添加子节点：" + event.getData().getPath());
                        System.out.println("子节点数据：" + new String(event.getData().getData()));
                    } else {
                        System.out.println("添加不正确...");
                    }
                } else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    System.out.println("删除子节点：" + event.getData().getPath());
                } else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    System.out.println("修改子节点路径：" + event.getData().getPath());
                    System.out.println("修改子节点数据：" + new String(event.getData().getData()));
                }
            }
        });

        Thread.sleep(3000);
        
        cto.closeZkClient();
        boolean started1 = cto.client.isStarted();
        System.out.println("当前客户端的状态：" + (started ? "连接中" : "已关闭"));
    }
}
