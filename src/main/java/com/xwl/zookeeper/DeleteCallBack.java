package com.xwl.zookeeper;

import org.apache.zookeeper.AsyncCallback;

/**
 * @author xwl
 * @date 2019-09-26 15:59
 * @description 删除节点回调事件
 */
public class DeleteCallBack implements AsyncCallback.VoidCallback {
    @Override
    public void processResult(int rc, String path, Object ctx) {
        System.out.println("删除节点" + path);
        System.out.println((String)ctx);
    }
}
