package cn.uncode.mq.client.consumer;

import java.util.List;

import cn.uncode.mq.network.Topic;

public interface ConsumerSubscriber {
	

	/**
	 * 订阅的主题
	 * @return
	 */
	List<String> subscribeToTopic();
	
	
	/**
	 * 通知
	 * @param topics
	 */
	void notify(Topic topic);

}
