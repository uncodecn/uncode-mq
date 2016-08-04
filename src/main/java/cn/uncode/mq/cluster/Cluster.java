/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.uncode.mq.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.lang3.StringUtils;

/**
 * The set of active brokers in the cluster
 * 
 * @author juny.ye
 */
public class Cluster{

	private static final Queue<Group> MASTER_BROKER_GROUP = new LinkedBlockingDeque<Group>();
	private static final Set<String> MASTER_BROKER_IP = new HashSet<String>();
	private static final ConcurrentHashMap<String, List<Group>> SLAVE_BROKER = new ConcurrentHashMap<>();//<queueName,List>
	private static final ConcurrentHashMap<String, Set<String>> HOSTS_QUEUES = new ConcurrentHashMap<>();//<host,queue>
	private static Group master;
	

    public static void addGroup(Group group){
    	MASTER_BROKER_GROUP.add(group);
    	MASTER_BROKER_IP.add(group.getMaster().getHost());
    }
    
    public static Set<String> getMasterIps(){
    	return MASTER_BROKER_IP;
    }
    
    public static void addGroups(List<Group> groups){
    	if(groups != null){
    		MASTER_BROKER_GROUP.addAll(groups);
        	for(Group gp:groups){
        		MASTER_BROKER_IP.add(gp.getMaster().getHost());
        	}
    	}
    }
    
    public static void addHostQueueName(String host, String queueName){
    	Set<String> queues = HOSTS_QUEUES.get(host);
    	if(queues == null){
    		queues = new HashSet<String>();
    		HOSTS_QUEUES.put(host, queues);
    	}
    	queues.add(queueName);
    }
    
    public static void clear(){
    	MASTER_BROKER_GROUP.clear();
    	MASTER_BROKER_IP.clear();
    	SLAVE_BROKER.clear();
    }
    
    public static void clearHostQueue(){
    	HOSTS_QUEUES.clear();
    }
    
    /**
     * 取新的可用组，慎用
     * @return
     */
    public static void peekAndSetMaster(){
    	setMaster(MASTER_BROKER_GROUP.peek());
    }
    
    
    public static Group peek(){
    	return MASTER_BROKER_GROUP.peek();
    }
    
    
    public static Set<String> getQueuesByServerHost(String host){
    	return HOSTS_QUEUES.get(host);
    }
    
    /**
     * 
     * @param queueName
     * @return ip:queue name array
     */
    public static Map<Broker, List<String>> getCustomerServerByQueues(String[] queueName){
    	Map<Broker, List<String>> hostsMap = new HashMap<>();
    	if(null != queueName){
    		for(String name:queueName){
    			if(SLAVE_BROKER.containsKey(name)){
    				List<Group> groups = SLAVE_BROKER.get(name);
    				Broker found = null;
    				for(Group gp:groups){
    					if(StringUtils.isNotBlank(gp.getMaster().getShost())){
    						found = gp.getMaster();
    						break;
    					}
    				}
    				if(found == null && groups.size() > 0){
    					found = groups.get(0).getMaster();
    				}
    				if(null != found){
    					if(hostsMap.get(found) == null){
    						List<String> list = new ArrayList<String>();
    						list.add(name);
    						hostsMap.put(found, list);
    					}else{
    						hostsMap.get(found).add(name);
    					}
    				}
    	    	}
    		}
    	}
    	return hostsMap;
    }
    
	public static Group getMaster() {
		return master;
	}

	public static void setMaster(Group master) {
		Cluster.master = master;
	}

	public static void putSlave(String queueName, String masterHost, String slaveHost){
		String masterIp = masterHost;
		boolean notFind = true;
		for(Group group : MASTER_BROKER_GROUP){
			if(group.getMaster().getHost().equals(masterIp)){
				List<Group> groups = SLAVE_BROKER.get(queueName);
				if(groups == null){
					groups = new ArrayList<Group>();
					groups.add(group.clone());
					SLAVE_BROKER.put(queueName, groups);
				}else{
					SLAVE_BROKER.get(queueName).add(group.clone());
				}
				notFind = false;
				break;
			}
		}
		if(notFind){
			Group gp = new Group();
			Group temp = MASTER_BROKER_GROUP.peek();
			Broker slave = null;
			boolean slaveFlag = false;
			if(StringUtils.isNoneBlank(slaveHost)){
				if(MASTER_BROKER_IP.contains(slaveHost)){
					slave = new Broker(slaveHost, temp.getMaster().getPort());
					slaveFlag = true;
				}
			}
			if(slaveFlag){
				gp.setSlaveOf(slave);
				List<Group> groups = SLAVE_BROKER.get(queueName);
				if(groups == null){
					groups = new ArrayList<Group>();
					groups.add(gp);
					SLAVE_BROKER.put(queueName, groups);
				}else{
					SLAVE_BROKER.get(queueName).add(gp);
				}
			}
		}
	}

    @Override
    public String toString() {
        return "Cluster(" + MASTER_BROKER_GROUP.toArray() + ")";
    }
}
