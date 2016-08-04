package cn.uncode.mq.network;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Topic implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String topic;
	
	private List<Object> contents = new ArrayList<Object>();

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public List<Object> getContents() {
		return contents;
	}
	
	public void addContent(Object content){
		contents.add(content);
	}

	public String toString(){
		return String.format("topic:%s,content:%s", topic, contents.toString());
	}
	
	

}
