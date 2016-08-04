
package cn.uncode.mq;

import java.util.Properties;

import org.apache.log4j.BasicConfigurator;
import org.junit.Before;
import org.junit.Test;

import cn.uncode.mq.server.NettyServer;
import cn.uncode.mq.server.RequestHandler;
import cn.uncode.mq.server.handlers.FetchRequestHandler;
import cn.uncode.mq.server.handlers.ProducerRequestHandler;
import cn.uncode.mq.server.handlers.ReplicaRequestHandler;


public class ServerTest {
	
	@Before
	public void before(){
		BasicConfigurator.configure();
	}

    @Test
    public void testCreateServer() {
        
    }
    
    public static void main(String[] args) throws InterruptedException{
    	BasicConfigurator.configure();
        NettyServer nettyServer = new NettyServer();
        Properties config = new Properties();
        config.setProperty("host", "192.168.1.43");
        config.setProperty("port", "9000");
        config.setProperty("replica.host", "192.168.7.131");
//        config.setProperty("replica.master", "192.168.7.131");
//        config.setProperty("replica.hosts", "192.168.7.131");
        config.setProperty("log.dir", "./data");
        config.setProperty("enable.zookeeper", "true");
        config.setProperty("zk.connect", "192.168.1.14:2181");
        config.setProperty("zk.username", "admin");
        config.setProperty("zk.password", "password");
        nettyServer.start(config);
		nettyServer.registerHandler(RequestHandler.FETCH, new FetchRequestHandler());
		nettyServer.registerHandler(RequestHandler.PRODUCER, new ProducerRequestHandler());
		nettyServer.registerHandler(RequestHandler.REPLICA, new ReplicaRequestHandler());
		nettyServer.waitForClose();
    }
}
