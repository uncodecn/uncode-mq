
package cn.uncode.mq;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import cn.uncode.mq.client.producer.Producer;
import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.network.Topic;





public class ProducerTest {
	
	@Before
	public void before(){
		BasicConfigurator.configure();
	}

    @Test
    public void testProducer() throws InterruptedException {
    	try {
    		Properties config = new Properties();
            config.setProperty("enable.zookeeper", "true");
            config.setProperty("zk.connect", "192.168.1.14:2181");
            config.setProperty("zk.username", "admin");
            config.setProperty("zk.password", "password");
    		config.setProperty("port", "9000");
            ServerConfig serverConfig = new ServerConfig(config);
			Producer.getInstance().connect(serverConfig);
			List<Topic> list = new ArrayList<Topic>();
			Topic topic = new Topic();
			topic.setTopic("test");
			topic.addContent("sssssssssssssssssssssss");
			list.add(topic);
			Producer.send(list);
			
			Producer.getInstance().stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
    
    public static void main(String[] args){
    	BasicConfigurator.configure();
    	try {
    		Properties config = new Properties();
    		config.setProperty("port", "9000");
            config.setProperty("zk.connect", "192.168.1.14:2181");
            config.setProperty("enable.zookeeper", "true");
            //config.setProperty("hostname", "127.0.0.1");
    		ServerConfig serverConfig = new ServerConfig(config);
            Producer.getInstance().connect(serverConfig);
            for(int i=0;i<1000000;i++){
            	List<Topic> list = new ArrayList<Topic>();
    			Topic topic = new Topic();
    			topic.setTopic("test");
    			topic.addContent("sssssssssssssssssssssss=>"+i);
    			list.add(topic);
    			Producer.send(list);
            }
			Producer.getInstance().stop();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
