package cn.uncode.mq.server.handlers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.store.TopicQueue;
import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.util.DataUtils;

/**
 * MQ消费处理器
 *
 * @author : juny.ye
 */
public class FetchReplicaRequestHandler implements RequestHandler {
	
	private final static int FETCH_SIZE = 10;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(FetchReplicaRequestHandler.class);
	
	@Override
	public Message handler(Message request) {
		List<Topic> result = new ArrayList<Topic>();
		Set<String> topics = (Set<String>) DataUtils.deserialize(request.getBody());
		if(topics != null){
			for(String topic:topics){
				TopicQueue queue = TopicQueuePool.getQueue(topic);
				if(null != queue){
					byte[] tpc = null;
					for(int i=0;i<FETCH_SIZE;i++){
						tpc = queue.poll();
						if(null != tpc){
							Topic tmp = (Topic) DataUtils.deserialize(tpc);
							result.add(tmp);
						}else{
							break;
						}
					}
				}
			}
		}
		Message response = Message.newResponseMessage();
		response.setSeqId(request.getSeqId());
		if(result.size() > 0){
			response.setBody(DataUtils.serialize(result));
		}
		LOGGER.info("Fetch request handler, message:"+result.toString());
		return response;
	}


	

	
}
