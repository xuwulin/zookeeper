package com.xwl.zookeeper.curator;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * @author xwl
 * @date 2019-09-26 18:21
 * @description zookeeper的watcher
 */
public class MyWatcher implements Watcher {
    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println("触发watcher，节点路劲为：" + watchedEvent.getPath());
    }
}
