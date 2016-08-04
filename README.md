# uncode-mq

java轻量级消息中间件。



# 功能特点

1. 消息存储速度非常快速。
2. 使用简单方便。
3. 依赖java环境。

说明：目前只在部分项目中使用。

------------------------------------------------------------------------

# 模块架构




------------------------------------------------------------------------

# 部署

1 解压umq-*.tar.gz到任意目录。

2 配置信息

  在conf/config.properties文件中填写相关信息。
  
	mq.host=192.168.1.43 #本机ip
	mq.port=9000 #端口
	mq.replica.host=192.168.7.131 #本机作为备机的主机ip
	mq.replica.fetch.size=100 #每次备份时同步的数据条数，默认30
	mq.replica.fetch.interval=2 #备份同步时间间隔，默认2秒
	mq.log.dir=./data #数据存储目录，默认data,不建议修改
	mq.data.persistence.interval=2 #数据持久化的时间间隔，默认2秒
	mq.enable.zookeeper=true #是否使用zk,集群环境下必须使用
	mq.zk.connect=192.168.1.14:2181 #zk地址
	mq.zk.username=admin #zk用户名
	mq.zk.password=password #zk密码
	mq.zk.connectiontimeout.ms=6000 #zk连接超时时间
	mq.zk.sessiontimeout.ms=6000 #zk连接session过期时间
	mq.zk.data.persistence.interval=6000 #zk数据同步时间，默认6秒

3 启动执行startup.sh，停止执行shutdown.sh，查看运行状态执行status.sh，查看主题信息执行info.sh，清除zk相关信息执行zkclear.sh。

4 目录

  umq/conf 配置
  umq/data 数据存储
  umq/logs 日志
  umq/lib 依赖jar
  
------------------------------------------------------------------------	
  
# 生产者

生产者为单例，必须最少执行一次connect操作，连接成功后不会重复connect。

	String cfg = "file:/gitlib/uncode-mq/conf/config.properties";
	Producer.getInstance().connect(cfg);
	for(int i=0;i<10000;i++){
		List<Topic> list = new ArrayList<Topic>();
		Topic topic = new Topic();
		topic.setTopic("umq");
		topic.addContent("umq作者juny=>"+i);
		list.add(topic);
		Producer.getInstance().send(list);
	}
	
	或
	
	Properties config = new Properties();
	config.setProperty("mq.port", "9000");
	config.setProperty("mq.zk.connect", "192.168.1.14:2181");
	config.setProperty("mq.enable.zookeeper", "true");
	ServerConfig serverConfig = new ServerConfig(config);
	Producer.getInstance().connect(serverConfig);
	for(int i=0;i<10000;i++){
		List<Topic> list = new ArrayList<Topic>();
		Topic topic = new Topic();
		topic.setTopic("umq");
		topic.addContent("umq作者juny=>"+i);
		list.add(topic);
		Producer.getInstance().send(list);
	}
------------------------------------------------------------------------	

# 消费者

1 普通方式

	String cfg = "file:/gitlib/uncode-mq/conf/config.properties";
	Consumer.runningConsumerRunnable(cfg);
	Consumer.addSubscriber(new ConsumerSubscriber(){
	
		//订阅主题
		@Override
		public List<String> subscribeToTopic() {
			List<String> tps = new ArrayList<String>();
			tps.add("umq");
			return tps;
		}
		
		//通知
		@Override
		public void notify(Topic topic) {
			System.err.println("consumer subscriber:"+topic.toString());
		}
		
	});
	
2 与spring集成

	@Service
	public class MyConsumerSubscriber implements ConsumerSubscriber {
	
		public static final String CFG = "file:/gitlib/uncode-mq/conf/config.properties";
	
		@Autowired
		LogService logServiceImpl;
		
		public ExpressRecordConsumerSubscriber() {
			//注册订阅者
			try {
				Consumer.runningConsumerRunnable(CFG);
				Consumer.addSubscriber(this);
			} catch (ConnectException e) {
				e.printStackTrace();
			}
		}

		//订阅主题
		@Override
		public List<String> subscribeToTopic() {
			List<String> tps = new ArrayList<String>();
			tps.add("umq");
			return tps;
		}

		@Override
		public void notify(Topic topic) {
			//处理逻辑
		}
		
	}
	
------------------------------------------------------------------------	


# 关于

作者：冶卫军（ywj_316@qq.com,微信:yeweijun）

技术支持QQ群：47306892

Copyright 2013 www.uncode.cn