package cn.uncode.mq.client.consumer;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
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

public class Consumer extends NettyClient {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
	
	private static final Consumer INSTANCE = new Consumer();
	
	private Map<Broker, Consumer> consumers = new HashMap<Broker, Consumer>();
	private Lock lock = new ReentrantLock(true);
	private Set<ConsumerSubscriber> subscribers = new HashSet<ConsumerSubscriber>();
	private Set<String> topics = new HashSet<String>();
	private ConsumerRunnable consumerRunnableThread;
	
	private Consumer(){}
	
	public static Consumer getInstance(){
		return INSTANCE;
	}
	
	public void loadClusterFromZK(ServerConfig config){
		initZkClient(config);
	}
	
	public void connect(ServerConfig config) throws ConnectException{
		if(config.getEnableZookeeper()){
			INSTANCE.loadClusterFromZK(config);
			INSTANCE.zkClient.subscribeChildChanges(ServerRegister.ZK_BROKER_GROUP, new ZkChildListener(){
				@Override
				public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
					ZkUtils.getCustomerCluster(zkClient, INSTANCE.topics.toArray(new String[0]));
					consumers.clear();
				}
			});
		}
		if(config.getTopics() != null){
			for(String topic:config.getTopics()){
				topics.add(topic);
			}
		}
	}
	
	public boolean reConnect(){
		if(!connected){
			ZkUtils.getCustomerCluster(INSTANCE.zkClient, INSTANCE.topics.toArray(new String[0]));
			Map<Broker, List<String>>  ipWithTopics = Cluster.getCustomerServerByQueues(INSTANCE.topics.toArray(new String[0]));
			if(null != ipWithTopics){
				for(Entry<Broker, List<String>> entry:ipWithTopics.entrySet()){
					if(!consumers.containsKey(entry.getKey())){
						try {
							Consumer consumer = new Consumer();
							consumer.open(entry.getKey().getHost(), entry.getKey().getPort());
							consumers.put(entry.getKey(), consumer);
							connected = true;
						} catch (Exception e) {
							LOGGER.error(String.format("connect %s:%d error：", entry.getKey(), entry.getKey().getPort()));
						}
					}
				}
				
			}
		}
		return connected;
	}
	
	public static void fetch(){
		if(INSTANCE.topics.size() > 0){
			if(INSTANCE.reConnect()){
				Map<Broker, List<String>>  ipWithTopics = Cluster.getCustomerServerByQueues(INSTANCE.topics.toArray(new String[0]));
				if(null != ipWithTopics && ipWithTopics.size() > 0){
					for(Entry<Broker, List<String>> entry:ipWithTopics.entrySet()){
						try {
							Consumer consumer = INSTANCE.consumers.get(entry.getKey());
							if(consumer != null){
								consumer.fetch(entry.getValue());
							}else{
								INSTANCE.connected = false;
								INSTANCE.consumers.clear();
							}
						} catch (Exception e) {
							INSTANCE.connected = false;
							INSTANCE.consumers.clear();
							LOGGER.error(e.getMessage(), e);
						}
					}
				}else{
					INSTANCE.connected = false;//没有相应的topic
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
				Message response = write(request);
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
						for (ConsumerSubscriber subscriber : INSTANCE.subscribers) {
							if(subscriber.subscribeToTopic() != null && subscriber.subscribeToTopic().contains(topic.getTopic())){
								subscriber.notify(topic);
							}
						}
					}
				} finally {
					lock.unlock();
				}
			}
		}
		return rtTopics;
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
	
	public static void runningConsumerRunnable(ServerConfig config) throws ConnectException{
		if (INSTANCE.consumerRunnableThread != null) {
			INSTANCE.consumerRunnableThread.close();
		}
		INSTANCE.consumerRunnableThread = new ConsumerRunnable(config);
		INSTANCE.consumerRunnableThread.start();
	}

	
	
	

}
