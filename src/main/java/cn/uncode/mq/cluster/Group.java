package cn.uncode.mq.cluster;

import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

public class Group implements Serializable {
	
	public static final String QUEUE_INDEX_PREFIX = "ms-";

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String name;
	
	private Broker master;
	
	private Broker slaveOf;
	
	public Group(){
	}
	
	public Group(String name, String hostname, int port){
		this(name, hostname, port, null);
	}
	
	public Group(String name, String hostname, int port, String replicaHost){
		String groupName = "ser";
		if(StringUtils.isNotBlank(name)){
			groupName = name;
		}
        this.setName(groupName + "-");
        this.master = new Broker(hostname, port);
        if(StringUtils.isNotBlank(replicaHost)){
        this.slaveOf = new Broker(replicaHost, port);
        }
	}

	public Broker getMaster() {
		return master;
	}

	public void setMaster(Broker master) {
		this.master = master;
	}

	public Broker getSlaveOf() {
		return slaveOf;
	}

	public void setSlaveOf(Broker slave) {
		this.slaveOf = slave;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Group clone(){
		Group newGroup = null;
		if(this.slaveOf == null){
			newGroup = new Group(name, master.getHost(), master.getPort());
		}else{
			newGroup = new Group(name, master.getHost(), master.getPort(), this.slaveOf.getHost());
		}
		newGroup.getMaster().setShost(this.master.getShost());
		return newGroup;
	}
	
	public String getZkIndexMasterSlave(){
		StringBuilder sb = new StringBuilder(QUEUE_INDEX_PREFIX);
		if(master != null && StringUtils.isNotBlank(master.getHost())){
			sb.append(master.getHost()).append(":");
		}
//		if(slaves.size() == 0){
//			sb.insert(0, "127.0.0.1:");
//		}else{
		if(slaveOf != null && StringUtils.isNotBlank(slaveOf.getHost())){
			sb.append(slaveOf.getHost()).append(":");
		}
//		}
		sb.deleteCharAt(sb.lastIndexOf(":"));
		return sb.toString();
	}
	
	public String toString(){
		return String.format("name:%s,master:[%s],slaveOf:[%s]", name, master==null?"":master.toString(), slaveOf==null?"":slaveOf.toString());
	}



	

	
	
	

}
