package cn.uncode.mq.client.producer;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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

public class Producer {
	
	private static final Producer INSTANCE = new Producer();
	
	private final static Logger LOGGER = LoggerFactory.getLogger(Producer.class);
	
	private NettyClient client = null;
	
	/**
     * 错误队列
     */
    private BlockingQueue<Topic> errorQueue = new LinkedBlockingQueue<Topic>();
	
	private Producer(){}
	
	public static Producer getInstance(){
		return INSTANCE;
	}
	
	public  void connect(String path) throws ConnectException{
		if(client == null){
			File mainFile = null;
			try {
				URL url = new URL(path);
				mainFile = new File(url.getFile()).getCanonicalFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (!mainFile.isFile() || !mainFile.exists()) {
				LOGGER.error(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
	            System.exit(2);
	        }
	        final ServerConfig config = new ServerConfig(mainFile);
	        connect(config);
		}
	}
	
	
	public  void connect(ServerConfig config) throws ConnectException{
		if(client == null){
			client = new NettyClient();
			if(config.getEnableZookeeper()){
				loadClusterFromZK(config);
				client.zkClient.subscribeChildChanges(ServerRegister.ZK_BROKER_GROUP,  new ZkChildListener(){
					@Override
					public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
						ZkUtils.getCluster(client.zkClient);
					}
				});
			}else{
				Group brokerGroup = new Group(config.getBrokerGroupName(), config.getHost(), config.getPort());
				Cluster.setCurrent(brokerGroup);
			}
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
				client.open(master.getHost(), master.getPort());
				break;
			}catch (IllegalStateException e) {
				client.stop();
				client = new NettyClient();
				count++;
				ZkUtils.getCluster(client.zkClient);
				LOGGER.error(String.format("producer connect %s:%d error：", master.getHost(), master.getPort()));
			} catch (Exception e) {
				count++;
				ZkUtils.getCluster(client.zkClient);
				LOGGER.error(String.format("producer connect %s:%d error：", master.getHost(), master.getPort()));
			}
		}while(count < 2 && !client.connected);
		return client.connected;
	}
	
	public boolean send(Topic topic){
		return send(new Topic[]{topic});
	}
	
	public boolean send(Topic topic, String... topicNames){
		List<Topic> topics = new ArrayList<Topic>();
		if(null != topicNames){
			for(String tp:topicNames){
				Topic top = new Topic();
				top.setTopic(tp);
				top.getContents().addAll(topic.getContents());
				topics.add(top);
			}
		}
		return send(topics);
	}
	
	public boolean send(Topic[] topics){
		return send(Arrays.asList(topics));
	}
	
	public boolean send(List<Topic> topics) throws SendRequestException{
		boolean result = false;
		if(reConnect()){
			Message request = Message.newRequestMessage();
			request.setReqHandlerType(RequestHandler.PRODUCER);
			request.setBody(DataUtils.serialize(topics));
			boolean errorFlag = false;
			try {
				Message response = client.write(request);
				if (response == null || response.getType() == TransferType.EXCEPTION.value) {
					errorQueue.addAll(topics);
					result = false;
					errorFlag = true;
				} else {
					result = true;
				}
			} catch (TimeoutException e) {
				client = new NettyClient();
				errorQueue.addAll(topics);
				errorFlag = true;
				if(!reConnect()){
					throw new SendRequestException("Prouder connection error");
				}
			} catch (SendRequestException e) {
				client = new NettyClient();
				errorQueue.addAll(topics);
				errorFlag = true;
				if(!reConnect()){
					throw new SendRequestException("Prouder connection error");
				}
			}
			
			if(!errorFlag){
				if(errorQueue.size() > 0){
					try {
						Topic t = errorQueue.poll(100, TimeUnit.MILLISECONDS);
						send(t);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}else{
			errorQueue.addAll(topics);
		}
		return result;
	}
	
	
	public void stop() {
		client.stop();
	}
	
	
	private void loadClusterFromZK(ServerConfig config){
		client.initZkClient(config);
		if(config.getEnableZookeeper()){
			ZkUtils.getCluster(client.zkClient);
		}
	}
	
	
	
	

}
