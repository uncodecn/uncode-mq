package cn.uncode.mq.server.backup;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.client.NettyClient;
import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.exception.SendRequestException;
import cn.uncode.mq.exception.TimeoutException;
import cn.uncode.mq.network.Backup;
import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.network.TransferType;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.util.DataUtils;
import cn.uncode.mq.util.Scheduler;

/**
 * 
 * @author juny.ye
 *
 */
public class EmbeddedConsumer{
	
	private static final EmbeddedConsumer INSTANCE = new EmbeddedConsumer();
    
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedConsumer.class);
    
    private final Scheduler scheduler = new Scheduler(1, "uncode-mq-embedded-consumer-", false);
    String replicaHost;
	int port;
	NettyClient replicaConsumer;
    Set<String> topics = new HashSet<String>();
    
	
    private EmbeddedConsumer(){
	}
    
    public static EmbeddedConsumer getInstance(){
		return INSTANCE;
    }
    
    public void start(ServerConfig config) {
    	replicaConsumer = new NettyClient();
    	replicaHost = config.getReplicaHost();
    	port = config.getPort();
    	if(StringUtils.isNotBlank(replicaHost)){
    		replicaConsumer = new NettyClient();
    		if (this.scheduler != null) {
                this.scheduler.scheduleWithRate(new ReplicaConsumerRunnable(), 30 * 1000, config.getReplicaFetchInterval() * 1000L);
            }
        	LOGGER.info("Embedded consumer started, replica host:" + replicaHost.toString());
    	}
    }

	public void stop(){
		replicaConsumer.stop();
		this.scheduler.shutdown();
	}
	
	class ReplicaConsumerRunnable implements Runnable {
		
		int connectError = 2;
	    
	    @Override
	    public void run() {
	    	if(connectError > 2){
	    		if(INSTANCE.replicaConsumer != null){
	    			INSTANCE.replicaConsumer.stop();
	    		}
	    		INSTANCE.replicaConsumer = new NettyClient();
	    		connectError = 0;
	    	}
	    	
    		if(!INSTANCE.replicaConsumer.connected){
    			try {
    				INSTANCE.replicaConsumer.open(INSTANCE.replicaHost, INSTANCE.port);
    			} catch (IllegalStateException e) {
    				connectError++;
    				LOGGER.error("Embedded consumer connection error.", e);
    			} catch (TimeoutException e) {
    				LOGGER.error("Embedded consumer connection error.", e);
    			} catch (Exception e) {
    				LOGGER.error("Embedded consumer connection error.", e);
    			}
			}else{
				Message request = Message.newRequestMessage();
				request.setReqHandlerType(RequestHandler.REPLICA);
				Set<String> queues = BackupQueuePool.getBackupQueueNameFromDisk();
				if(queues != null){
					List<Backup> backups = new ArrayList<Backup>();
					for(String queue:queues){
						BackupQueue backupQueue = BackupQueuePool.getBackupQueueFromPool(queue);
						int wNum = backupQueue.getWriteIndex().getWriteNum();
						int wPosition = backupQueue.getWriteIndex().getWritePosition();
						int wCounter = backupQueue.getWriteIndex().getWriteCounter();
						backups.add(new Backup(queue, wNum, wPosition, wCounter));
					}
					request.setBody(DataUtils.serialize(backups));
				}
				
				try {
					Message response = INSTANCE.replicaConsumer.write(request);
					if (response.getType() == TransferType.EXCEPTION.value) {
					} else {
						if(response.getBody() != null){
							List<Topic> backupResult = (List<Topic>) DataUtils.deserialize(response.getBody());
							if(backupResult != null && backupResult.size() > 0){
								for(Topic topic:backupResult){
									BackupQueue backupQueue = BackupQueuePool.getBackupQueueFromPool(topic.getTopic());
									backupQueue.offer(DataUtils.serialize(topic));
								}
							}
						}
					}
					if(response.getBody().length > 0){
						LOGGER.info("Fetch topic from master broker, size:"+response.getBody().length);
					}
				} catch (SendRequestException e) {
					INSTANCE.replicaConsumer.setConnected(false);
					LOGGER.error("Embedded consumer flush topic error.", e);
				}catch (Exception e) {
					INSTANCE.replicaConsumer.setConnected(false);
					connectError++;
					LOGGER.error("Embedded consumer flush topic error.", e);
				}
				
			}

	    }

		

	}
	
	
	

}
