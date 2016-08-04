package cn.uncode.mq.server.handlers;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.network.Backup;
import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.server.AbstractRequestHandler;
import cn.uncode.mq.store.TopicQueue;
import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.util.DataUtils;

/**
 * MQ生产者处理器
 *
 * @author : juny.ye
 */
public class ReplicaRequestHandler extends AbstractRequestHandler {
	
	public ReplicaRequestHandler(ServerConfig config) {
		super(config);
	}

	private final static int FETCH_SIZE = 10;
	
	private final static Logger LOGGER = LoggerFactory.getLogger(ReplicaRequestHandler.class);
	
	@Override
	public Message handler(Message request) {
		List<Topic> result = new ArrayList<Topic>();
		List<Backup> backups = (List<Backup>) DataUtils.deserialize(request.getBody());
		if(backups != null){
			for(Backup backup:backups){
				TopicQueue queue = TopicQueuePool.getQueue(backup.getQueueName());
				if(null != queue){
					byte[] tpc = null;
					int rnum = backup.getSlaveWriteNum();
					int rposition = backup.getSlaveWritePosition();
					for(int i=0;i<FETCH_SIZE;i++){
						tpc = queue.replicaRead(rnum, rposition);
						if(null != tpc){
							rposition = rposition + tpc.length + 4;
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
