package cn.uncode.mq.server.backup;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.client.NettyClient;
import cn.uncode.mq.cluster.Cluster;
import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.network.Backup;
import cn.uncode.mq.network.BackupResult;
import cn.uncode.mq.network.Message;
import cn.uncode.mq.network.Topic;
import cn.uncode.mq.network.TransferType;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.server.ServerRegister;
import cn.uncode.mq.util.DataUtils;
import cn.uncode.mq.util.Scheduler;
import cn.uncode.mq.util.ZkUtils;
import cn.uncode.mq.zk.ZkChildListener;
import cn.uncode.mq.zk.ZkClient;

/**
 * 
 * @author juny.ye
 *
 */
public class EmbeddedConsumer{
	
	private static final EmbeddedConsumer INSTANCE = new EmbeddedConsumer();
	
    /**
     * 日志拉取时间间隔(单位:秒)
     */
    private static final int REPLICA_FETJ_INTERVAL = 5;
    

    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedConsumer.class);
    
    private final Scheduler scheduler = new Scheduler(1, "uncode-mq-server-producer-", false);
    String replicaHost;
	int port;
	ZkClient zkClient;
    ReplicaConsumer replicaConsumer;
    Set<String> topics = new HashSet<String>();
    
	
    private EmbeddedConsumer(){
	}
    
    public static EmbeddedConsumer getInstance(){
		return INSTANCE;
    }
    
    public void start(ServerConfig config, ZkClient zkClient) {
    	replicaHost = config.getReplicaHost();
    	port = config.getPort();
    	this.zkClient = zkClient;
    	if(StringUtils.isNotBlank(replicaHost)){
    		replicaConsumer = new ReplicaConsumer(replicaHost, port, zkClient);
    		if (this.scheduler != null) {
        		LOGGER.debug("starting topic producer " + 3000 + " ms");
                this.scheduler.scheduleWithRate(new ReplicaConsumerRunnable(), 30 * 1000, REPLICA_FETJ_INTERVAL * 1000L);
            }
        	LOGGER.info("Embedded consumer started, replica host:" + replicaHost.toString());
    	}
//    	zkClient.subscribeChildChanges(ServerRegister.ZK_BROKER_GROUP, new ZkChildListener(){
//			@Override
//			public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception {
//				ZkUtils.getCustomerCluster(zkClient, INSTANCE.topics.toArray(new String[0]));
//			}
//		});
    	
    }

	public void stop(){
		replicaConsumer.stop();
		this.scheduler.shutdown();
	}
	
	class ReplicaConsumer extends NettyClient{
		
		final String host;
		final int port;
		final ZkClient zkClient;
		
		public ReplicaConsumer(String host, int port, ZkClient zkClient){
			this.host = host;
			this.port = port;
			this.zkClient = zkClient;
		}
		
		@Override
		public void connect(ServerConfig config) throws ConnectException {
			//nothing
		}

		@Override
		public boolean reConnect() {
			try {
				if(!connected){
					this.open(host, port);
					connected = true;
				}
			} catch (Exception e) {
				connected = false;
				this.stop();
			}
			return connected;
		}
	}

	class ReplicaConsumerRunnable implements Runnable {
		
		int connectError = 0;
	    
	    @Override
	    public void run() {
	    	if(connectError > 4){
	    		replicaConsumer = new ReplicaConsumer(replicaHost, port, zkClient);
	    		connectError = 0;
	    	}
			if(replicaConsumer.reConnect()){
				Message request = Message.newRequestMessage();
				request.setReqHandlerType(RequestHandler.REPLICA);
				ZkUtils.loadEmbeddedCustomerCluster(replicaConsumer.zkClient, replicaConsumer.host);
				Set<String> queues = Cluster.getQueuesByServerHost(replicaConsumer.host);
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
					try {
						Message response = replicaConsumer.write(request);
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
						LOGGER.info("Fetch topic from master broker, size:"+response.getBody().length);
					} catch (Exception e) {
						replicaConsumer.setConnected(false);
						LOGGER.error("Embedded consumer flush topic error.", e);
					}
				}
				
			}else{
				connectError++;
			}

	    }

		

	}
	
	
	

}
