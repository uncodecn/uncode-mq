
package cn.uncode.mq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import cn.uncode.mq.client.consumer.Consumer;
import cn.uncode.mq.client.consumer.ConsumerSubscriber;
import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.network.Topic;


public class ConsumerTest {
	
	@Before
	public void before(){
		BasicConfigurator.configure();
	}

    @Test
    public void testConsumer() throws InterruptedException {
    	try {
    		Properties config = new Properties();
    		config.setProperty("host", "192.168.1.43");
            config.setProperty("port", "9000");
            config.setProperty("replica.hosts", "192.168.7.131");
            config.setProperty("zk.connect", "192.168.1.14:2181");
            config.setProperty("enable.zookeeper", "true");
            ServerConfig serverConfig = new ServerConfig(config);
			Consumer.getInstance().connect(serverConfig);
//			Consumer.fetch(new String[]{"test"});
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			Consumer.getInstance().stop();
		}
    }
    
    public static void main(String[] args){
    	BasicConfigurator.configure();
		try {
			Properties config = new Properties();
//    		config.setProperty("host", "192.168.1.43");
            config.setProperty("port", "9000");
//            config.setProperty("replica.hosts", "192.168.7.131");
            config.setProperty("zk.connect", "192.168.1.14:2181");
            config.setProperty("enable.zookeeper", "true");
            ServerConfig serverConfig = new ServerConfig(config);
			Consumer.runningConsumerRunnable(serverConfig);
			Consumer.addSubscriber(new ConsumerSubscriber(){

				@Override
				public List<String> subscribeToTopic() {
					List<String> tps = new ArrayList<String>();
					tps.add("test");
					return tps;
				}

				@Override
				public void notify(Topic topic) {
					System.err.println("consumer subscriber:"+topic.toString());
				}
				
			});
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
