package cn.uncode.mq.server.handlers;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.util.DataUtils;

/**
 * MQ生产者处理器
 *
 * @author : juny.ye
 */
public class ProducerRequestHandler implements RequestHandler {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ProducerRequestHandler.class);
	
	@Override
	public Message handler(Message request) {
		if(null != request.getBody()){
			List<Topic> topics = (List<Topic>) DataUtils.deserialize(request.getBody());
			if(topics != null){
				for(Topic topic:topics){
					TopicQueuePool.getQueueOrCreate(topic.getTopic()).offer(DataUtils.serialize(topic));
//					EmbeddedConsumer.getInstance().push(topic);
				}
				LOGGER.info("Producer request handler, receive message:"+topics.toString());
			}else{
				LOGGER.info("Producer request handler, receive message is null.");
			}
		}
		Message response = Message.newResponseMessage();
		response.setSeqId(request.getSeqId());
		return response;
	}


	

	
}
