package cn.uncode.mq.server;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.config.ServerConfig;
import cn.uncode.mq.serializer.NettyDecoder;
import cn.uncode.mq.serializer.NettyEncoder;
import cn.uncode.mq.server.backup.EmbeddedConsumer;
import cn.uncode.mq.store.TopicQueuePool;
import cn.uncode.mq.util.PortUtils;
import cn.uncode.mq.zk.ZkClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.collection.IntObjectHashMap;
import io.netty.util.collection.IntObjectMap;

/**
 * @author : juny.ye
 */
public class NettyServer {
	private static final Logger LOGGER = LoggerFactory.getLogger(NettyServer.class);

	private EventLoopGroup bossGroup;
	private EventLoopGroup workerGroup;
	private ChannelFuture f;

	private final IntObjectMap<RequestHandler> handlerMap = new IntObjectHashMap<>(128);
//	private final EmbeddedConsumer embeddedConsumer = new EmbeddedConsumer();
	
	private ServerRegister serverRegister;
	
	public void start(int port){
		Properties properties = new Properties();
        properties.setProperty("port",port+"");
		start(properties);
	}
	
	public void start(String configFileName) {
        File mainFile = null;
		try {
			mainFile = new File(configFileName).getCanonicalFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        if (!mainFile.isFile() || !mainFile.exists()) {
            System.err.println(String.format("ERROR: Main config file not exist => '%s', copy one from 'conf/server.properties.sample' first.", mainFile.getAbsolutePath()));
            System.exit(2);
        }
        final ServerConfig config = new ServerConfig(configFileName);
        start(config);
    }
	
	public void start(Properties config){
		start(new ServerConfig(config));
	}

	public void start(ServerConfig config) {
		LOGGER.info("Netty Server is starting");
		PortUtils.checkAvailablePort(config.getPort());
		
		ServerBootstrap b = configServer();
		try {
			// start server
			if(StringUtils.isNotBlank(config.getHost())){
				f = b.bind(config.getHost(), config.getPort()).sync();
			}else{
				f = b.bind(config.getPort()).sync();
			}
			// register shutown hook
			Runtime.getRuntime().addShutdownHook(new ShutdownThread());

		} catch (Exception e) {
			LOGGER.error("Exception happen when start server", e);
		}
		
		//zk
		ZkClient zkClient = null;
		if(config.getEnableZookeeper()){
			serverRegister = new ServerRegister();
			zkClient = serverRegister.startup(config);
		}
		
		TopicQueuePool.startup(zkClient, config.getDataDir());
		
		if(null != config.getReplicaHost()){
			EmbeddedConsumer.getInstance().start(config, zkClient);
		}
		
	}

	public void registerHandler(int handlerId, RequestHandler requestHandler) {
		handlerMap.put(handlerId, requestHandler);
	}

	/**
	 * blocking to wait for close.
	 */
	public void waitForClose() throws InterruptedException {
		f.channel().closeFuture().sync();
	}

	public void stop() {
		LOGGER.info("Netty server is stopping");
		
		bossGroup.shutdownGracefully();
		workerGroup.shutdownGracefully();
		if(f.channel() != null){
			f.channel().close();
		}
		EmbeddedConsumer.getInstance().stop();

		LOGGER.info("Netty server stoped");
	}

	private ServerBootstrap configServer() {
		bossGroup = new NioEventLoopGroup(2);
		workerGroup = new NioEventLoopGroup(0);

		ServerBootstrap b = new ServerBootstrap();
		b.group(bossGroup, workerGroup)
				.channel(NioServerSocketChannel.class)
				.option(ChannelOption.SO_BACKLOG, 1024).option(ChannelOption.TCP_NODELAY, true)
				.option(ChannelOption.SO_TIMEOUT, 6000)
				.childOption(ChannelOption.SO_REUSEADDR, true).childOption(ChannelOption.SO_KEEPALIVE, true)
				.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

		b.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ch.pipeline().addLast(
						//new LoggingHandler("SERVLER", LogLevel.INFO),
						new NettyDecoder(),
						new NettyEncoder(),
						new NettyServerHandler(handlerMap)
				);
			}
		});

		return b;
	}

	class ShutdownThread extends Thread {
		@Override
		public void run() {
			NettyServer.this.stop();
		}
	}
}
