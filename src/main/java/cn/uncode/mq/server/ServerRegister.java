package cn.uncode.mq.server;


import static  cn.uncode.mq.util.ZkUtils.ZK_MQ_BASE;

import java.io.Closeable;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.cluster.Cluster;
import cn.uncode.mq.cluster.Group;
import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.exception.ZkNodeExistsException;
import cn.uncode.mq.util.DataUtils;
import cn.uncode.mq.util.ZkUtils;
import cn.uncode.mq.zk.ZkChildListener;
import cn.uncode.mq.zk.ZkClient;

/**
 * Handles the server's interaction with zookeeper. The server needs to register the following
 * paths:
 * <p/>
 * <pre>
 *   /uncode/mq/brokergroup/
 * </pre>
 *
 * @author juny.ye
 */
public class ServerRegister implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerRegister.class);
    
    public static final String ZK_BROKER_GROUP = ZK_MQ_BASE + "/brokergroup";

    private ServerConfig config;
    
    private ZkClient zkClient;

    public ZkClient startup(ServerConfig config) {
    	LOGGER.info("connecting to zookeeper: " + config.getZkConnect());
    	this.config = config;
    	String authString = config.getZkUsername() + ":"+ config.getZkPassword();
        zkClient = new ZkClient(config.getZkConnect(), authString, config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs());
        registerBrokerGroupInZk();
        return zkClient;
    }


    /**
     * register broker group in the zookeeper
     * <p>
     * path: /uncode/mq/brokergroup/<id> <br/>
     * data: json
     * </p>
     */
    public void registerBrokerGroupInZk() {
    	String zkPath = ZK_BROKER_GROUP;
    	ZkUtils.makeSurePersistentPathExists(zkClient, zkPath);
    	LOGGER.info("Registering broker group" + zkPath);
        //
        Group brokerGroup = new Group(config.getBrokerGroupName(), config.getHost(), config.getPort(), config.getReplicaHost());
        zkPath += "/" + brokerGroup.getName();
        String jsonGroup = DataUtils.brokerGroup2Json(brokerGroup);
        try {
//        	ZkUtils.getCluster(zkClient);
            ZkUtils.createEphemeralPathExpectConflict(zkClient, zkPath, jsonGroup);
            Cluster.setMaster(brokerGroup);//暂存，index中使用
        } catch (ZkNodeExistsException e) {
            String oldServerInfo = ZkUtils.readDataMaybeNull(zkClient, zkPath);
            String message = "A broker (%s) is already registered on the path %s." //
                    + " This probably indicates that you either have configured a brokerid that is already in use, or "//
                    + "else you have shutdown this broker and restarted it faster than the zookeeper " ///
                    + "timeout so it appears to be re-registering.";
            message = String.format(message, oldServerInfo, zkPath);
            throw new RuntimeException(message);
        }
        //
        LOGGER.info("Registering broker group" + zkPath + " succeeded with " + jsonGroup);
    }

    /**
     *
     */
    public void close() {
        if (zkClient != null) {
        	LOGGER.info("closing zookeeper client...");
            zkClient.close();
        }
    }
    
}
