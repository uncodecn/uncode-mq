package cn.uncode.mq.zk;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.exception.ZkException;


public class ZkConnection {


    private static final Logger LOG = LoggerFactory.getLogger(ZkConnection.class);

    private ZooKeeper _zk = null;
    private final Lock _zookeeperLock = new ReentrantLock();

    private final String _servers;
    private final int _sessionTimeOut;
    private final String _authStr;
    private final List<ACL> acl = new ArrayList<ACL>();

    private static final Method method;

    static {
        Method[] methods = ZooKeeper.class.getDeclaredMethods();
        Method m = null;
        for (Method method : methods) {
            if (method.getName().equals("multi")) {
                m = method;
                break;
            }
        }
        method = m;
    }

    /**
     * build a zookeeper connection
     * @param zkServers      zookeeper connection string
     * @param sessionTimeOut session timeout in milliseconds
     */
    public ZkConnection(String zkServers, int sessionTimeOut, String authStr) {
        _servers = zkServers;
        _sessionTimeOut = sessionTimeOut;
        _authStr = authStr;
    }

    public void connect(Watcher watcher){
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                throw new IllegalStateException("zk client has already been started");
            }
            try {
                LOG.debug("Creating new ZookKeeper instance to connect to " + _servers + ".");
                _zk = new ZooKeeper(_servers, _sessionTimeOut, watcher);
                if(StringUtils.isNotBlank(_authStr)){
                	_zk.addAuthInfo("digest", _authStr.getBytes());
                	acl.clear();
                    try {
						acl.add(new ACL(ZooDefs.Perms.ALL, new Id("digest",
						        DigestAuthenticationProvider.generateDigest(_authStr))));
					} catch (NoSuchAlgorithmException e) {
						e.printStackTrace();
					}
                    acl.add(new ACL(ZooDefs.Perms.READ, Ids.ANYONE_ID_UNSAFE));
                }
            } catch (IOException e) {
                throw new ZkException("Unable to connect to " + _servers, e);
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }

    public void close() throws InterruptedException {
        _zookeeperLock.lock();
        try {
            if (_zk != null) {
                LOG.debug("Closing ZooKeeper connected to " + _servers);
                _zk.close();
                _zk = null;
            }
        } finally {
            _zookeeperLock.unlock();
        }
    }
    
    public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, acl, mode);
    }

    /*public String create(String path, byte[] data, CreateMode mode) throws KeeperException, InterruptedException {
        return _zk.create(path, data, Ids.OPEN_ACL_UNSAFE, mode);
    }*/

    public void delete(String path) throws InterruptedException, KeeperException {
        _zk.delete(path, -1);
    }

    public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
        return _zk.exists(path, watch) != null;
    }

    public List<String> getChildren(final String path, final boolean watch) throws KeeperException, InterruptedException {
        return _zk.getChildren(path, watch);
    }

    public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
        return _zk.getData(path, watch, stat);
    }

    /**
     * wrapper for 3.3.x/3.4.x
     *
     * @param ops multi operations
     * @return OpResult list
     */
    public List<?> multi(Iterable<?> ops) {
        if (method == null) throw new UnsupportedOperationException("multi operation must use zookeeper 3.4+");
        try {
            return (List<?>) method.invoke(_zk, ops);
        } catch (IllegalArgumentException e) {
            throw new UnsupportedOperationException("ops must be 'org.apache.zookeeper.Op'");
        } catch (IllegalAccessException e) {
            throw new UnsupportedOperationException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public Stat writeData(String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return _zk.setData(path, data, version);
    }

    public States getZookeeperState() {
        return _zk != null ? _zk.getState() : null;
    }

    public long getCreateTime(String path) throws KeeperException, InterruptedException {
        Stat stat = _zk.exists(path, false);
        if (stat != null) {
            return stat.getCtime();
        }
        return -1;
    }

    public String getServers() {
        return _servers;
    }

    public ZooKeeper getZooKeeper() {
        return _zk;
    }

}
