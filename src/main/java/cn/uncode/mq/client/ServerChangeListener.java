package cn.uncode.mq.client;

import java.util.List;

import cn.uncode.mq.util.ZkUtils;
import cn.uncode.mq.zk.ZkChildListener;
import cn.uncode.mq.zk.ZkClient;

public class ServerChangeListener implements ZkChildListener{
	
	private final ZkClient zkClient;
	
	public ServerChangeListener(ZkClient zkClient){
		this.zkClient = zkClient;
	}

	@Override
	public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
		ZkUtils.getCluster(zkClient);
		
	}

	



}
