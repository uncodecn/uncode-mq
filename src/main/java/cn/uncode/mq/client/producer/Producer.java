package cn.uncode.mq.client.producer;

import java.net.ConnectException;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.client.NettyClient;
import cn.uncode.mq.cluster.Broker;
import cn.uncode.mq.cluster.Cluster;
import cn.uncode.mq.cluster.Group;
import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.exception.SendRequestException;
import cn.uncode.mq.exception.TimeoutException;
import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.network.TransferType;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.server.ServerRegister;
import cn.uncode.mq.util.DataUtils;
import cn.uncode.mq.util.ZkUtils;
import cn.uncode.mq.zk.ZkChildListener;

public class Producer extends NettyClient {
	
	private static final Producer INSTANCE = new Producer();
	
	private final static Logger LOGGER = LoggerFactory.getLogger(Producer.class);
	
	private Producer(){}
	
	
	public void loadClusterFromZK(ServerConfig config){
		initZkClient(config);
		if(config.getEnableZookeeper()){
			ZkUtils.getCluster(zkClient);
		}
	}
	
	public static Producer getInstance(){
		return INSTANCE;
	}
	
	public  void connect(ServerConfig config) throws ConnectException{
		if(config.getEnableZookeeper()){
			INSTANCE.loadClusterFromZK(config);
			INSTANCE.zkClient.subscribeChildChanges(ServerRegister.ZK_BROKER_GROUP,  new ZkChildListener(){
				@Override
				public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
					ZkUtils.getCluster(zkClient);
				}
			});
		}else{
			Group brokerGroup = new Group(config.getBrokerGroupName(), config.getHost(), config.getPort());
			Cluster.setMaster(brokerGroup);
		}
		if(!reConnect()){
			throw new ConnectException("Producer connect error");
		}
	}
	
	
	public boolean reConnect(){
		int count = 0;
		do{
			Group group = Cluster.peek();
			if(null == group){
				return false;
			}
			Broker master = group.getMaster();
			try {
				INSTANCE.open(master.getHost(), master.getPort());
				break;
			} catch (Exception e) {
				LOGGER.error(String.format("connect %s:%d error：", master.getHost(), master.getPort()));
				count++;
				ZkUtils.getCluster(zkClient);
			}
		}while(count < 2 && !connected);
		return connected;
	}
	
	public static boolean send(Topic[] topics){
		return send(Arrays.asList(topics));
	}
	
	public static boolean send(List<Topic> topics) throws SendRequestException{
		boolean result = false;
		Message request = Message.newRequestMessage();
		request.setReqHandlerType(RequestHandler.PRODUCER);
		request.setBody(DataUtils.serialize(topics));
		try {
			Message response = INSTANCE.write(request);
			if (response.getType() == TransferType.EXCEPTION.value) {
				result = false;
			} else {
				result = true;
			}
		} catch (TimeoutException e) {
			if(!INSTANCE.reConnect()){
				throw new SendRequestException("Prouder connection error");
			}
		} catch (SendRequestException e) {
			if(!INSTANCE.reConnect()){
				throw new SendRequestException("Prouder connection error");
			}
		}
		
		return result;
	}
	
	
	
	
	
	
	

}
