package cn.uncode.mq.server;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import cn.uncode.mq.server.handlers.FetchRequestHandler;
import cn.uncode.mq.server.handlers.ProducerRequestHandler;
import cn.uncode.mq.server.handlers.ReplicaRequestHandler;

public class UMQ {
	
	private static final String PARAM_KEY_PORT = "-p";
	private static final String PARAM_KEY_REPLICA = "-replica";
	private static final String PARAM_KEY_LOG_DIR = "-dir";
	private static final String PARAM_KEY_HOST = "-h";


    public static void main(String[] args) {
    	int port = 9999;
    	String replica = null;
    	String dir = null;
    	String host = null;
    	try {
	        int argsSize = args.length;
	        if (argsSize > 0) {
	        	if(argsSize % 2 != 0){
	                System.out.println("参数数量不对");
	                System.exit(1);
	        	}
	        	int i = 0;
	        	while(i < argsSize){
	        		if(StringUtils.isBlank(args[i]) || StringUtils.isBlank(args[i+1])){
	        			continue;
	        		}
	        		if(PARAM_KEY_PORT.equals(args[i].toLowerCase())){
	        			port = Integer.valueOf(args[i+1]);
	        		}else if(PARAM_KEY_REPLICA.equals(args[i].toLowerCase())){
	        			replica = String.valueOf(args[i+1]);
	        		}else if(PARAM_KEY_LOG_DIR.equals(args[i].toLowerCase())){
	        			dir = String.valueOf(args[i+1]);
	        		}else if(PARAM_KEY_HOST.equals(args[i].toLowerCase())){
	        			host = String.valueOf(args[i+1]);
	        		}
	        		i += 2;
	        	}
	        }
	        NettyServer nettyServer = new NettyServer();
	        Properties config = new Properties();
	        config.setProperty("host", host);
	        config.setProperty("port", port+"");
	        if(StringUtils.isNoneBlank(replica)){
	        	config.setProperty("replica.host", replica);
	        }
	        if(StringUtils.isNoneBlank(dir)){
	        	config.setProperty("log.dir", dir);
	        }
	        nettyServer.start(config);
			nettyServer.registerHandler(RequestHandler.FETCH, new FetchRequestHandler());
			nettyServer.registerHandler(RequestHandler.PRODUCER, new ProducerRequestHandler());
			nettyServer.registerHandler(RequestHandler.REPLICA, new ReplicaRequestHandler());
			nettyServer.waitForClose();
		} catch (InterruptedException e) {
			System.err.println(e);
		}
        

    }
}
