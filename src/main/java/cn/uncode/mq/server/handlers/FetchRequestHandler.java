package cn.uncode.mq.server.handlers;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.server.backup.BackupQueue;
import cn.uncode.mq.server.backup.BackupQueuePool;
import cn.uncode.mq.store.TopicQueue;
import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.util.DataUtils;

/**
 * MQ消费处理器
 *
 * @author : juny.ye
 */
public class FetchRequestHandler implements RequestHandler {
	
	private final static Logger LOGGER = LoggerFactory.getLogger(FetchRequestHandler.class);
	
	@Override
	public Message handler(Message request) {
		List<Topic> result = new ArrayList<Topic>();
		List<Topic> topics = (List<Topic>) DataUtils.deserialize(request.getBody());
		if(topics != null){
			for(Topic topic:topics){
				int readCounter = 0;
				byte[] tpc = null;
				boolean backupQueueOver = false;
				BackupQueue backupQueue = BackupQueuePool.getBackupQueueFromPool(topic.getTopic());
				if(null != backupQueue){
					tpc = backupQueue.poll();
					readCounter = backupQueue.getReadIndex().getReadCounter();
					if(tpc == null && readCounter == backupQueue.getWriteIndex().getWriteCounter()){
						backupQueueOver = true;
					}
				}
				if(backupQueue == null || backupQueueOver){
					TopicQueue queue = TopicQueuePool.getQueue(topic.getTopic());
					if(null != queue){
						tpc = queue.poll();
						readCounter = queue.getReadIndex().getReadCounter();
					}
				}
				if(null != tpc){
					Topic tmp = (Topic) DataUtils.deserialize(tpc);
					tmp.setReadCounter(readCounter);
					result.add(tmp);
				}
			}
		}
		Message response = Message.newResponseMessage();
		response.setSeqId(request.getSeqId());
		if(result.size() > 0){
			response.setBody(DataUtils.serialize(result));
			LOGGER.info("Fetch request handler, message:"+result.toString());
		}
		return response;
	}


	

	
}
