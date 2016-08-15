package cn.uncode.mq.client.consumer;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.client.NettyClient;
import cn.uncode.mq.cluster.Broker;
import cn.uncode.mq.cluster.Cluster;
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
import cn.uncode.mq.zk.ZkClient;

public class Consumer{
	
	private final static Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
	
	private static final Consumer INSTANCE = new Consumer();
	private static final Map<String, Integer> COUNTER = new HashMap<>();
	private static int  ZK_COUNER_MAX = 5;
	
//	private Map<Broker, Consumer> consumers = new HashMap<Broker, Consumer>();
	private Lock lock = new ReentrantLock(true);
	private Set<ConsumerSubscriber> subscribers = new HashSet<ConsumerSubscriber>();
	private Set<String> topics = new HashSet<String>();
	public ZkClient zkClient = null;
	private int zkCounter = 0;

	NettyClient client = new NettyClient();
	private ConsumerRunnable consumerRunnableThread;
	
	private Consumer(){}
	
	public static Consumer getInstance(){
		return INSTANCE;
	}
	
	public void loadClusterFromZK(ServerConfig config){
		client.initZkClient(config);
		zkClient = client.zkClient;
	}
	
	public void connect(ServerConfig config) throws ConnectException{
		if(config.getEnableZookeeper()){
			loadClusterFromZK(config);
			client.zkClient.subscribeChildChanges(ServerRegister.ZK_BROKER_GROUP, new ZkChildListener(){
				@Override
				public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
					ZkUtils.getCustomerCluster(client.zkClient, INSTANCE.topics.toArray(new String[0]));
				}
			});
		}
		if(config.getTopics() != null){
			for(String topic:config.getTopics()){
				topics.add(topic);
			}
		}
		ZK_COUNER_MAX = config.getZKDataPersistenceInterval()/2;
		zkCounter = ZK_COUNER_MAX;
	}
	
	public boolean reConnect(){
		if(!client.connected){
			if(client != null && client.zkClient != null){
				if(INSTANCE.zkCounter >= ZK_COUNER_MAX){
					ZkUtils.getCustomerCluster(client.zkClient, topics.toArray(new String[0]));
					client.stop();
					client = new NettyClient();
					client.zkClient = zkClient;
					INSTANCE.zkCounter = 0;
				}
				Map<Broker, List<String>>  ipWithTopics = Cluster.getCustomerServerByQueues(topics.toArray(new String[0]));
				if(null != ipWithTopics){
					Broker[] brokers = ipWithTopics.keySet().toArray(new Broker[0]);
					if(brokers != null && brokers.length > 0){
						try {
							client.open(brokers[0].getHost(), brokers[0].getPort());
						} catch (IllegalStateException e) {
							client.connected = false;
							INSTANCE.zkCounter++;
							LOGGER.error(String.format("consumer %s:%d error：", brokers[0].getHost(), brokers[0].getPort()));
						} catch (Exception e) {
							client.connected = false;
							LOGGER.error(String.format("consumer %s:%d error：", brokers[0].getHost(), brokers[0].getPort()));
						}
					}
					int last = (int) (System.currentTimeMillis()%10);
					if(last > 5){
						INSTANCE.zkCounter++;
					}
				}else{
					INSTANCE.zkCounter++;
				}
			}
		}
		return client.connected;
	}
	
	public static void fetch(){
		if(INSTANCE.topics.size() > 0){
			if(INSTANCE.reConnect()){
				try {
					INSTANCE.fetch(INSTANCE.topics.toArray(new String[0]));
				} catch (Exception e) {
					INSTANCE.client.connected = false;
					LOGGER.error(e.getMessage(), e);
				}
			}
		}
	}
	
	public List<Topic> fetch(String[] topics)throws TimeoutException, SendRequestException{
		return fetch(Arrays.asList(topics));
	}
	
	public List<Topic> fetch(List<String> topics) throws TimeoutException, SendRequestException{
		List<Topic> rtTopics = null;
		if(topics != null && topics.size() > 0){
			List<Topic> topicList = new ArrayList<Topic>();
			for(String tp : topics){
				Topic topic = new Topic();
				topic.setTopic(tp);
				topicList.add(topic);
			}
			Message request = Message.newRequestMessage();
			request.setReqHandlerType(RequestHandler.FETCH);
			request.setBody(DataUtils.serialize(topicList));
			try {
				Message response = client.write(request);
				if (response.getType() == TransferType.EXCEPTION.value) {
					// 有异常
					LOGGER.error("Cuonsumer fetch message error");
				} else {
					if(null != response.getBody()){
						rtTopics = (List<Topic>) DataUtils.deserialize(response.getBody());
					}
				}
			} catch (TimeoutException e) {
				throw e;
			} catch (SendRequestException e) {
				throw e;
			}
			
			//通知订阅者
			if(rtTopics != null){
				LOGGER.info("=>> Consumer fetch message:"+rtTopics.toString());
				lock.lock();
				try {
					for(Topic topic : rtTopics){
						if(COUNTER.containsKey(topic.getTopic())){
							if(topic.getReadCounter() > COUNTER.get(topic.getTopic())){
								for (ConsumerSubscriber subscriber : INSTANCE.subscribers) {
									if(subscriber.subscribeToTopic() != null && subscriber.subscribeToTopic().contains(topic.getTopic())){
										subscriber.notify(topic);
									}
								}
								COUNTER.put(topic.getTopic(), topic.getReadCounter());
							}else if(topic.getReadCounter() < 0){
								for (ConsumerSubscriber subscriber : INSTANCE.subscribers) {
									if(subscriber.subscribeToTopic() != null && subscriber.subscribeToTopic().contains(topic.getTopic())){
										subscriber.notify(topic);
									}
								}
								COUNTER.put(topic.getTopic(), topic.getReadCounter());
							}else{
								LOGGER.info("Has been spending, " + topic.toString());
							}
						}else{
							for (ConsumerSubscriber subscriber : INSTANCE.subscribers) {
								if(subscriber.subscribeToTopic() != null && subscriber.subscribeToTopic().contains(topic.getTopic())){
									subscriber.notify(topic);
								}
							}
							COUNTER.put(topic.getTopic(), topic.getReadCounter());
						}
						
					}
				} finally {
					lock.unlock();
				}
			}
		}
		return rtTopics;
	}
	
	public void stop() {
		client.stop();
	}
	
	public static void addSubscriber(ConsumerSubscriber subscriber){
		INSTANCE.lock.lock();
		try {
			if (subscriber != null){
				if(!INSTANCE.subscribers.contains(subscriber)){
					INSTANCE.subscribers.add(subscriber);
				}
				INSTANCE.topics.addAll(subscriber.subscribeToTopic());
			}
		} finally {
			INSTANCE.lock.unlock();
		}
	}
	
	public static void deleteSubscriber(ConsumerSubscriber subscriber) {
		INSTANCE.lock.lock();
		try {
			if (subscriber != null){
				if(INSTANCE.subscribers.contains(subscriber)){
					INSTANCE.subscribers.remove(subscriber);
				}
				INSTANCE.topics.removeAll(subscriber.subscribeToTopic());
			}
		} finally {
			INSTANCE.lock.unlock();
		}
	}
	
	public static void runningConsumerRunnable(String path) throws ConnectException{
		File mainFile = null;
		try {
			URL url = new URL(path);
			mainFile = new File(url.getFile()).getCanonicalFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (!mainFile.isFile() || !mainFile.exists()) {
            System.err.println(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
            System.exit(2);
        }
		ServerConfig serverConfig = new ServerConfig(mainFile);
		runningConsumerRunnable(serverConfig);
	}
	
	public static void runningConsumerRunnable(ServerConfig config) throws ConnectException{
		if(INSTANCE.consumerRunnableThread == null){
			INSTANCE.consumerRunnableThread = new ConsumerRunnable(config);
			INSTANCE.consumerRunnableThread.start();
		}
	}

	
	
	

}
