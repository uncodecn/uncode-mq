package cn.uncode.mq.client.consumer;

import java.util.List;

import cn.uncode.mq.network.Topic;

public interface ConsumerSubscriber {
	

	/**
	 * 订阅的主题
	 * @return 主题列表
	 */
	List<String> subscribeToTopic();
	
	
	/**
	 * 通知
	 * @param topic 主题
	 */
	void notify(Topic topic);

}
