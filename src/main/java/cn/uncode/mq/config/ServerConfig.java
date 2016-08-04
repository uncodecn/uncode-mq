package cn.uncode.mq.config;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;


public class ServerConfig extends Config {

    public ServerConfig(Properties props) {
        super(props);
    }
    
    public ServerConfig(String filename){
    	 super(filename);
    }
    
    /**
     * 端口号
     */
    public int getPort() {
        return getInt("port", 9092);
    }

    /**
     * Broker的ip地址
     */
    public String getHost() {
        return getString("host", null);
    }

    /**
     * 备份节点
     * @return
     */
    public String getReplicaHost(){
    	return getString("replica.host", null);
    }
    
    /** ZK host string */
    public String getZkConnect() {
        return getString("zk.connect", null);
    }
    
    public String getZkUsername() {
        return getString("zk.username", "");
    }
    
    public String getZkPassword() {
        return getString("zk.password", "");
    }

    /** zookeeper session timeout */
    public int getZkSessionTimeoutMs() {
        return getInt("zk.sessiontimeout.ms", 6000);
    }

    /**
     * the max time that the client waits to establish a connection to
     * zookeeper
     */
    public int getZkConnectionTimeoutMs() {
        return getInt("zk.connectiontimeout.ms", 6000);
    }

    /** how far a ZK follower can be behind a ZK leader */
    public int getZkSyncTimeMs() {
        return getInt("zk.synctime.ms", 2000);
    }
    
    /**
     * data file path
     * @return
     */
    public String getDataDir(){
        return getString("data.dir", null);
    }
    
    
    /**
     * 订阅主题
     * @return
     */
    public String[] getTopics(){
    	String topics = getString("topics", null);
    	if(StringUtils.isNoneBlank(topics)){
    		return topics.split(",");
    	}
    	return null;
    }
    

    /**
     * topic.autocreated create new topic after booted
     * @return auto create new topic after booted, default true
     */
    public boolean isTopicAutoCreated(){
        return getBoolean("topic.autocreated",true);
    }

    /**
     * the broker group name for this server
     */
    public String getBrokerGroupName() {
        return getString("group.name", null);
    }

    /**
     * max connection for one jvm (default 10000)
     */
    public int getMaxConnections() {
        return getInt("max.connections", 10000);
    }
    
    /**
     * connection timeout (default 3000)
     */
    public int getConnectTimeoutMillis() {
        return getInt("connection.timeout", 3000);
    }

    /**
     * the SO_SNDBUFF buffer of the socket sever sockets
     */
    public int getSocketSendBuffer() {
        return getInt("socket.send.buffer", 100 * 1024);
    }

    /**
     * the SO_RCVBUFF buffer of the socket sever sockets
     */
    public int getSocketReceiveBuffer() {
        return getInt("socket.receive.buffer", 100 * 1024);
    }

    /**
     * the maximum number of bytes in a socket request
     */
    public int getMaxSocketRequestSize() {
        return getIntInRange("max.socket.request.bytes", 100 * 1024 * 1024, 1, Integer.MAX_VALUE);
    }

    /**
     * the number of worker threads that the server uses for handling all
     * client requests
     */
    public int getNumThreads() {
        return getIntInRange("num.threads", Runtime.getRuntime().availableProcessors(), 1, Integer.MAX_VALUE);
    }

    /**
     * the interpublic String get in which to measure performance
     * statistics
     */
    public int getMonitoringPeriodSecs() {
        return getIntInRange("monitoring.period.secs", 600, 1, Integer.MAX_VALUE);
    }

    /**
     * the default number of log partitions per topic
     */
    public int getNumPartitions() {
        return getIntInRange("num.partitions", 1, 1, Integer.MAX_VALUE);
    }

    /**
     * the directory in which the log data is kept
     */
    public String getLogDir() {
        return getString("log.dir");
    }

    /**
     * the maximum size of a single log file
     */
    public int getLogFileSize() {
        return 0;//getIntInRange("log.file.size", 1 * 1024 * 1024 * 1024, Message.MinHeaderSize, Integer.MAX_VALUE);
    }

    /**
     * Flush到硬盘的累计message大小
     * the number of messages accumulated on a log partition before
     * messages are flushed to disk
     */
    public int getFlushInterval() {
        return getIntInRange("log.flush.interval", 500, 1, Integer.MAX_VALUE);
    }

    /**
     * log文件删除前的保留时间
     * the number of hours to keep a log file before deleting it
     */
    public int getLogRetentionHours() {
        return getIntInRange("log.retention.hours", 24 * 7, 1, Integer.MAX_VALUE);
    }

    /**
     * log文件删除最大文件大小
     * the maximum size of the log before deleting it
     */
    public int getLogRetentionSize() {
        return getInt("log.retention.size", -1);
    }

    /**
     * 特定topic的删除保留时间
     * the number of hours to keep a log file before deleting it for some
     * specific topic
     */
    public Map<String, Integer> getLogRetentionHoursMap() {
        return getTopicRentionHours(getString("topic.log.retention.hours", ""));
    }

    /**
     * the frequency in minutes that the log cleaner checks whether any log
     * is eligible for deletion
     */
    public int getLogCleanupIntervalMinutes() {
        return getIntInRange("log.cleanup.interval.mins", 10, 1, Integer.MAX_VALUE);
    }

    /**
     * enable zookeeper registration in the server
     */
    public boolean getEnableZookeeper() {
        return getBoolean("enable.zookeeper", false);
    }

    /**
     * the maximum time in ms that a message in selected topics is kept in
     * memory before flushed to disk, e.g., topic1:3000,topic2: 6000
     */
    public Map<String, Integer> getFlushIntervalMap() {
        return getTopicFlushIntervals(getString("topic.flush.intervals.ms", ""));
    }

    /**
     * the frequency in ms that the log flusher checks whether any log
     * needs to be flushed to disk
     */
    public int getFlushSchedulerThreadRate() {
        return getInt("log.default.flush.scheduler.interval.ms", 3000);
    }

    /**
     * the maximum time in ms that a message in any topic is kept in memory
     * before flushed to disk
     */
    public int getDefaultFlushIntervalMs() {
        return getInt("log.default.flush.interval.ms", getFlushSchedulerThreadRate());
    }

    /**
     * the number of partitions for selected topics, e.g.,
     * topic1:8,topic2:16
     */
    public Map<String, Integer> getTopicPartitionsMap() {
        return getTopicPartitions(getString("topic.partition.count.map", ""));
    }

    /**
     * get the rolling strategy (default value is
     * {@link FixedSizeRollingStrategy})
     *
     * @return RollingStrategy Object
     */
   /* public RollingStrategy getRollingStrategy() {
        return Utils.getObject(getString("log.rolling.strategy", null));
    }*/

    /**
     * get Authentication method
     *
     * @return Authentication method
     * @see Authentication#build(String)
     */
//    public Authentication getAuthentication() {
//        return authentication;
//    }

    /**
     * maximum size of message that the server can receive (default 1MB)
     *
     * @return maximum size of message
     */
    public int getMaxMessageSize() {
        return getIntInRange("max.message.size", 1024 * 1024, 0, Integer.MAX_VALUE);
    }
    
    

    public Map<String, Integer> getTopicRentionHours(String retentionHours) {
        String exceptionMsg = "Malformed token for topic.log.retention.hours in server.properties: ";
        String successMsg = "The retention hour for ";
        return getCSVMap(retentionHours, exceptionMsg, successMsg);
    }

    public Map<String, Integer> getTopicFlushIntervals(String allIntervals) {
        String exceptionMsg = "Malformed token for topic.flush.Intervals.ms in server.properties: ";
        String successMsg = "The flush interval for ";
        return getCSVMap(allIntervals, exceptionMsg, successMsg);
    }


    public Map<String, Integer> getTopicPartitions(String allPartitions) {
        String exceptionMsg = "Malformed token for topic.partition.counts in server.properties: ";
        String successMsg = "The number of partitions for topic  ";
        return getCSVMap(allPartitions, exceptionMsg, successMsg);
    }

    public Map<String, Integer> getConsumerTopicMap(String consumerTopicString) {
        String exceptionMsg = "Malformed token for embeddedconsumer.topics in consumer.properties: ";
        String successMsg = "The number of consumer thread for topic  ";
        return getCSVMap(consumerTopicString, exceptionMsg, successMsg);
    }
    
    public Map<String, Integer> getCSVMap(String value, String exceptionMsg, String successMsg) {
        Map<String, Integer> map = new LinkedHashMap<String, Integer>();
        if (value == null || value.trim().length() < 3) return map;
        for (String one : value.trim().split(",")) {
            String[] kv = one.split(":");
            //FIXME: force positive number
            map.put(kv[0].trim(), Integer.valueOf(kv[1].trim()));
        }
        return map;
    }
}
