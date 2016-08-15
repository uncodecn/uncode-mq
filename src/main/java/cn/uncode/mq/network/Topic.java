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
	
	private int readCounter;
	
	private List<Serializable> contents = new ArrayList<Serializable>();

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}
	
	public List<Serializable> getContents() {
		return contents;
	}
	
	public void addContent(Serializable content){
		contents.add(content);
	}

	public int getReadCounter() {
		return readCounter;
	}

	public void setReadCounter(int readCounter) {
		this.readCounter = readCounter;
	}

	public String toString(){
		return String.format("topic:%s,counter:%d,content:%s", topic, readCounter, contents.toString());
	}
	
	

}
