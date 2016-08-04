package cn.uncode.mq.network;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class BackupResult implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -330577267934364039L;
	
	private List<Topic> topics;
	
	public BackupResult() {
		this.topics = new ArrayList<Topic>();
	}

	public List<Topic> getTopics() {
		return topics;
	}
    
    
    
    
    

}
