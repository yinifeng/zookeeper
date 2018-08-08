package com.hubo.zkclient.watcher;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;

public class ZkClientWatcher2 {

    private static final String CONNECT_ADDRESS="";

    private static final int SESSION_TIMEOUT=5000;

    private static final String PARENT_NODE="/zkclinet";
    
    
    public static void main(String[] args) throws Exception{
        ZkClient zkc=new ZkClient(new ZkConnection(CONNECT_ADDRESS),SESSION_TIMEOUT);
        
        zkc.createPersistent(PARENT_NODE,"1234");
        
        
        //添加节点数据监听
        zkc.subscribeDataChanges(PARENT_NODE, new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {
                System.out.println(String.format("变更的节点为：%s，变更的内容为：%s",dataPath,data));
            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                System.out.println(String.format("删除的节点为：%s",dataPath));
            }
        });
        
        Thread.sleep(3000);
        
        zkc.writeData(PARENT_NODE, "456",-1);
        Thread.sleep(1000);
        
        zkc.delete(PARENT_NODE);
        
        System.in.read();
    }
}
