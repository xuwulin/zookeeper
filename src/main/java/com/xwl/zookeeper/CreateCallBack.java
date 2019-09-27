package com.xwl.zookeeper;

import org.apache.zookeeper.AsyncCallback;

/**
 * @author xwl
 * @date 2019-09-26 15:47
 * @description 增加节点回调事件
 */
public class CreateCallBack implements AsyncCallback.StringCallback {
    @Override
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("创建节点：" + path);
        System.out.println((String) ctx);
    }
}
