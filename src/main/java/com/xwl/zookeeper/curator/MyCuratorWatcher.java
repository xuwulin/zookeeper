package com.xwl.zookeeper.curator;

import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.zookeeper.WatchedEvent;

/**
 * @author xwl
 * @date 2019-09-26 18:20
 * @description Curator的watcher
 */
public class MyCuratorWatcher implements CuratorWatcher {
    @Override
    public void process(WatchedEvent watchedEvent) throws Exception {
        System.out.println("触发watcher，节点路劲为：" + watchedEvent.getPath());
    }
}
