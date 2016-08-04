package cn.uncode.mq.zk;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.KeeperException.ConnectionLossException;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper.States;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.uncode.mq.exception.ZkBadVersionException;
import cn.uncode.mq.exception.ZkException;
import cn.uncode.mq.exception.ZkInterruptedException;
import cn.uncode.mq.exception.ZkNoNodeException;
import cn.uncode.mq.exception.ZkNodeExistsException;
import cn.uncode.mq.exception.ZkTimeoutException;
import cn.uncode.mq.zk.ZkEventThread.ZkEvent;



/**
 * Zookeeper client
 * <p>
 * The client is thread-safety
 * </p>
 */
public class ZkClient implements Watcher, Closeable {
	
    private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;

    private static final int DEFAULT_SESSION_TIMEOUT = 30000;

    private final static Logger LOG = LoggerFactory.getLogger(ZkClient.class);

    protected ZkConnection _connection;

    private final Map<String, Set<ZkChildListener>> _childListener = new ConcurrentHashMap<String, Set<ZkChildListener>>();

    private final Map<String, Set<ZkDataListener>> _dataListener = new ConcurrentHashMap<String, Set<ZkDataListener>>();

    private final Set<ZkStateListener> _stateListener = new CopyOnWriteArraySet<ZkStateListener>();

    private volatile KeeperState _currentState;

    private final ZkLock _zkEventLock = new ZkLock();

    private volatile boolean _shutdownTriggered;

    private ZkEventThread _eventThread;

    private Thread _zookeeperEventThread;

    /**
     * Create a client with default connection timeout and default session timeout
     *
     * @param connectString zookeeper connection string
     *                      comma separated host:port pairs, each corresponding to a zk
     *                      server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *                      the optional chroot suffix is used the example would look
     *                      like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *                      where the client would be rooted at "/app/a" and all paths
     *                      would be relative to this root - ie getting/setting/etc...
     *                      "/foo/bar" would result in operations being run on
     *                      "/app/a/foo/bar" (from the server perspective).
     * @see IZkClient#DEFAULT_CONNECTION_TIMEOUT
     * @see IZkClient#DEFAULT_SESSION_TIMEOUT
     */
    public ZkClient(String connectString, String authStr) {
        this(connectString, authStr, DEFAULT_CONNECTION_TIMEOUT);
    }

    /**
     * Create a client
     *
     * @param connectString     zookeeper connection string
     *                          comma separated host:port pairs, each corresponding to a zk
     *                          server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *                          the optional chroot suffix is used the example would look
     *                          like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *                          where the client would be rooted at "/app/a" and all paths
     *                          would be relative to this root - ie getting/setting/etc...
     *                          "/foo/bar" would result in operations being run on
     *                          "/app/a/foo/bar" (from the server perspective).
     * @param connectionTimeout connection timeout in milliseconds
     */
    public ZkClient(String connectString, String authStr, int connectionTimeout) {
        this(connectString, authStr, DEFAULT_SESSION_TIMEOUT, connectionTimeout);
    }

    /**
     * Create a client
     *
     * @param connectString     zookeeper connection string
     *                          comma separated host:port pairs, each corresponding to a zk
     *                          server. e.g. "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002" If
     *                          the optional chroot suffix is used the example would look
     *                          like: "127.0.0.1:3000,127.0.0.1:3001,127.0.0.1:3002/app/a"
     *                          where the client would be rooted at "/app/a" and all paths
     *                          would be relative to this root - ie getting/setting/etc...
     *                          "/foo/bar" would result in operations being run on
     *                          "/app/a/foo/bar" (from the server perspective).
     * @param sessionTimeout    session timeout in milliseconds
     * @param connectionTimeout connection timeout in milliseconds
     */
    public ZkClient(String connectString, String authStr, int sessionTimeout, int connectionTimeout) {
        this(new ZkConnection(connectString, sessionTimeout, authStr), connectionTimeout);
    }

    /**
     * Create a client with special implementation
     *
     * @param zkConnection      special client
     * @param connectionTimeout connection timeout in milliseconds
     */
    public ZkClient(ZkConnection zkConnection, int connectionTimeout) {
        _connection = zkConnection;
        connect(connectionTimeout, this);
    }


    public List<String> subscribeChildChanges(String path, ZkChildListener listener) {
        synchronized (_childListener) {
            Set<ZkChildListener> listeners = _childListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZkChildListener>();
                _childListener.put(path, listeners);
            }
            listeners.add(listener);
        }
        return watchForChilds(path);
    }

    public void unsubscribeChildChanges(String path, ZkChildListener childListener) {
        synchronized (_childListener) {
            final Set<ZkChildListener> listeners = _childListener.get(path);
            if (listeners != null) {
                listeners.remove(childListener);
            }
        }
    }

    public void subscribeDataChanges(String path, ZkDataListener listener) {
        Set<ZkDataListener> listeners;
        synchronized (_dataListener) {
            listeners = _dataListener.get(path);
            if (listeners == null) {
                listeners = new CopyOnWriteArraySet<ZkDataListener>();
                _dataListener.put(path, listeners);
            }
            listeners.add(listener);
        }
        watchForData(path);
        LOG.debug("Subscribed data changes for " + path);
    }

    public void unsubscribeDataChanges(String path, ZkDataListener dataListener) {
        synchronized (_dataListener) {
            final Set<ZkDataListener> listeners = _dataListener.get(path);
            if (listeners != null) {
                listeners.remove(dataListener);
            }
            if (listeners == null || listeners.isEmpty()) {
                _dataListener.remove(path);
            }
        }
    }

    public void subscribeStateChanges(final ZkStateListener listener) {
        synchronized (_stateListener) {
            _stateListener.add(listener);
        }
    }

    public void unsubscribeStateChanges(ZkStateListener stateListener) {
        synchronized (_stateListener) {
            _stateListener.remove(stateListener);
        }
    }

    public void unsubscribeAll() {
        synchronized (_childListener) {
            _childListener.clear();
        }
        synchronized (_dataListener) {
            _dataListener.clear();
        }
        synchronized (_stateListener) {
            _stateListener.clear();
        }
    }


    public void createPersistent(String path) {
        createPersistent(path, false);
    }


    public void createPersistent(String path, boolean createParents) {
        try {
            create(path, null, CreateMode.PERSISTENT);
        } catch (ZkNodeExistsException e) {
            if (!createParents) {
                throw e;
            }
        } catch (ZkNoNodeException e) {
            if (!createParents) {
                throw e;
            }
            String parentDir = path.substring(0, path.lastIndexOf('/'));
            createPersistent(parentDir, createParents);
            createPersistent(path, createParents);
        }
    }


    public void createPersistent(String path, byte[] data) {
        create(path, data, CreateMode.PERSISTENT);
    }


    public String createPersistentSequential(String path, byte[] data) {
        return create(path, data, CreateMode.PERSISTENT_SEQUENTIAL);
    }


    public void createEphemeral(final String path) {
        create(path, null, CreateMode.EPHEMERAL);
    }

    public String create(final String path, byte[] data, final CreateMode mode) {
        if (path == null) {
            throw new NullPointerException("path must not be null.");
        }
        final byte[] bytes = data;

        return retryUntilConnected(new Callable<String>() {

            @Override
            public String call() throws Exception {
                return _connection.create(path, bytes, mode);
            }
        });
    }

    public void createEphemeral(final String path, final byte[] data) {
        create(path, data, CreateMode.EPHEMERAL);
    }

    public String createEphemeralSequential(final String path, final byte[] data) {
        return create(path, data, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    public void process(WatchedEvent event) {
        LOG.debug("Received event: " + event);
        _zookeeperEventThread = Thread.currentThread();

        boolean stateChanged = event.getPath() == null;
        boolean znodeChanged = event.getPath() != null;
        boolean dataChanged = event.getType() == EventType.NodeDataChanged || //
                event.getType() == EventType.NodeDeleted ||
                event.getType() == EventType.NodeCreated || //
                event.getType() == EventType.NodeChildrenChanged;

        getEventLock().lock();
        try {

            // We might have to install child change event listener if a new node was created
            if (getShutdownTrigger()) {
                LOG.debug("ignoring event '{" + event.getType() + " | " + event.getPath() + "}' since shutdown triggered");
                return;
            }
            if (stateChanged) {
                processStateChanged(event);
            }
            if (dataChanged) {
                processDataOrChildChange(event);
            }
        } finally {
            if (stateChanged) {
                getEventLock().getStateChangedCondition().signalAll();

                // If the session expired we have to signal all conditions, because watches might have been removed and
                // there is no guarantee that those
                // conditions will be signaled at all after an Expired event
                if (event.getState() == KeeperState.Expired) {
                    getEventLock().getZNodeEventCondition().signalAll();
                    getEventLock().getDataChangedCondition().signalAll();
                    // We also have to notify all listeners that something might have changed
                    fireAllEvents();
                }
            }
            if (znodeChanged) {
                getEventLock().getZNodeEventCondition().signalAll();
            }
            if (dataChanged) {
                getEventLock().getDataChangedCondition().signalAll();
            }
            getEventLock().unlock();
            LOG.debug("Leaving process event");
        }
    }

    private void fireAllEvents() {
        for (Entry<String, Set<ZkChildListener>> entry : _childListener.entrySet()) {
            fireChildChangedEvents(entry.getKey(), entry.getValue());
        }
        for (Entry<String, Set<ZkDataListener>> entry : _dataListener.entrySet()) {
            fireDataChangedEvents(entry.getKey(), entry.getValue());
        }
    }

    public List<String> getChildren(String path) {
        return getChildren(path, hasListeners(path));
    }

    protected List<String> getChildren(final String path, final boolean watch) {
        try {
            return retryUntilConnected(new Callable<List<String>>() {

                @Override
                public List<String> call() throws Exception {
                    return _connection.getChildren(path, watch);
                }
            });
        } catch (ZkNoNodeException e) {
            return null;
        }
    }


    public int countChildren(String path) {
        try {
            Stat stat = new Stat();
            this.readData(path, stat);
            return stat.getNumChildren();
            //return getChildren(path).size();
        } catch (ZkNoNodeException e) {
            return -1;
        }
    }

    protected boolean exists(final String path, final boolean watch) {
        return retryUntilConnected(new Callable<Boolean>() {

            @Override
            public Boolean call() throws Exception {
                return _connection.exists(path, watch);
            }
        });
    }

    public boolean exists(final String path) {
        return exists(path, hasListeners(path));
    }

    private void processStateChanged(WatchedEvent event) {
        LOG.info("zookeeper state changed (" + event.getState() + ")");
        setCurrentState(event.getState());
        if (getShutdownTrigger()) {
            return;
        }
        try {
            fireStateChangedEvent(event.getState());

            if (event.getState() == KeeperState.Expired) {
                reconnect();
                fireNewSessionEvents();
            }
        } catch (final Exception e) {
            throw new RuntimeException("Exception while restarting zk client", e);
        }
    }

    private void fireNewSessionEvents() {
        for (final ZkStateListener stateListener : _stateListener) {
            _eventThread.send(new ZkEvent("New session event sent to " + stateListener) {

                @Override
                public void run() throws Exception {
                    stateListener.handleNewSession();
                }
            });
        }
    }

    private void fireStateChangedEvent(final KeeperState state) {
        for (final ZkStateListener stateListener : _stateListener) {
            _eventThread.send(new ZkEvent("State changed to " + state + " sent to " + stateListener) {

                @Override
                public void run() throws Exception {
                    stateListener.handleStateChanged(state);
                }
            });
        }
    }

    private boolean hasListeners(String path) {
        Set<ZkDataListener> dataListeners = _dataListener.get(path);
        if (dataListeners != null && dataListeners.size() > 0) {
            return true;
        }
        Set<ZkChildListener> childListeners = _childListener.get(path);
        if (childListeners != null && childListeners.size() > 0) {
            return true;
        }
        return false;
    }

    public boolean deleteRecursive(String path) {
        List<String> children;
        try {
            children = getChildren(path, false);
        } catch (ZkNoNodeException e) {
            return true;
        }

        for (String subPath : children) {
            if (!deleteRecursive(path + "/" + subPath)) {
                return false;
            }
        }

        return delete(path);
    }

    private void processDataOrChildChange(WatchedEvent event) {
        final String path = event.getPath();

        if (event.getType() == EventType.NodeChildrenChanged ||
                event.getType() == EventType.NodeCreated ||
                event.getType() == EventType.NodeDeleted) {
            Set<ZkChildListener> childListeners = _childListener.get(path);
            if (childListeners != null && !childListeners.isEmpty()) {
                fireChildChangedEvents(path, childListeners);
            }
        }

        if (event.getType() == EventType.NodeDataChanged ||
                event.getType() == EventType.NodeDeleted ||
                event.getType() == EventType.NodeCreated) {
            Set<ZkDataListener> listeners = _dataListener.get(path);
            if (listeners != null && !listeners.isEmpty()) {
                fireDataChangedEvents(event.getPath(), listeners);
            }
        }
    }

    private void fireDataChangedEvents(final String path, Set<ZkDataListener> listeners) {
        for (final ZkDataListener listener : listeners) {
            _eventThread.send(new ZkEvent("Data of " + path + " changed sent to " + listener) {

                @Override
                public void run() throws Exception {
                    // reinstall watch
                    exists(path, true);
                    try {
                        byte[] data = readData(path, null, true);
                        listener.handleDataChange(path, data);
                    } catch (ZkNoNodeException e) {
                        listener.handleDataDeleted(path);
                    }
                }
            });
        }
    }

    private void fireChildChangedEvents(final String path, Set<ZkChildListener> childListeners) {
        try {
            // reinstall the watch
            for (final ZkChildListener listener : childListeners) {
                _eventThread.send(new ZkEvent("Children of " + path + " changed sent to " + listener) {

                    @Override
                    public void run() throws Exception {
                        try {
                            // if the node doesn't exist we should listen for the root node to reappear
                            exists(path);
                            List<String> children = getChildren(path);
                            listener.handleChildChange(path, children);
                        } catch (ZkNoNodeException e) {
                            listener.handleChildChange(path, null);
                        }
                    }
                });
            }
        } catch (Exception e) {
            LOG.error("Failed to fire child changed event. Unable to getChildren.  ", e);
        }
    }

    public boolean waitUntilExists(String path, TimeUnit timeUnit, long time) throws ZkInterruptedException {
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));
        LOG.debug("Waiting until znode '" + path + "' becomes available.");
        if (exists(path)) {
            return true;
        }
        acquireEventLock();
        try {
            while (!exists(path, true)) {
                boolean gotSignal = getEventLock().getZNodeEventCondition().awaitUntil(timeout);
                if (!gotSignal) {
                    return false;
                }
            }
            return true;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    public boolean waitUntilConnected() throws ZkInterruptedException {
        return waitUntilConnected(Integer.MAX_VALUE, TimeUnit.MILLISECONDS);
    }

    public boolean waitUntilConnected(long time, TimeUnit timeUnit) throws ZkInterruptedException {
        return waitForKeeperState(KeeperState.SyncConnected, time, timeUnit);
    }

    public boolean waitForKeeperState(KeeperState keeperState, long time, TimeUnit timeUnit)
            throws ZkInterruptedException {
        if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        Date timeout = new Date(System.currentTimeMillis() + timeUnit.toMillis(time));

        LOG.debug("Waiting for keeper state " + keeperState);
        acquireEventLock();
        try {
            boolean stillWaiting = true;
            while (_currentState != keeperState) {
                if (!stillWaiting) {
                    return false;
                }
                stillWaiting = getEventLock().getStateChangedCondition().awaitUntil(timeout);
            }
            LOG.debug("State is " + _currentState);
            return true;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    private void acquireEventLock() {
        try {
            getEventLock().lockInterruptibly();
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        }
    }

    /**
     * @param callable the callable object
     * @return result of Callable
     * @throws ZkInterruptedException   if operation was interrupted, or a required reconnection
     *                                  got interrupted
     * @throws IllegalArgumentException if called from anything except the ZooKeeper event
     *                                  thread
     * @throws ZkException              if any ZooKeeper exception occurred
     * @throws RuntimeException         if any other exception occurs from invoking the Callable
     */
    public <E> E retryUntilConnected(Callable<E> callable) {
        if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        while (true) {
            try {
                return callable.call();
            } catch (ConnectionLossException e) {
                // we give the event thread some time to update the status to 'Disconnected'
                Thread.yield();
                waitUntilConnected();
            } catch (SessionExpiredException e) {
                // we give the event thread some time to update the status to 'Expired'
                Thread.yield();
                waitUntilConnected();
            } catch (KeeperException e) {
                throw ZkException.create(e);
            } catch (InterruptedException e) {
                throw new ZkInterruptedException(e);
            } catch (Exception e) {
                throw ZkClientUtils.convertToRuntimeException(e);
            }
        }
    }

    public void setCurrentState(KeeperState currentState) {
        getEventLock().lock();
        try {
            _currentState = currentState;
        } finally {
            getEventLock().unlock();
        }
    }

    /**
     * Returns a mutex all zookeeper events are synchronized aginst. So in case you need to do
     * something without getting any zookeeper event interruption synchronize against this
     * mutex. Also all threads waiting on this mutex object will be notified on an event.
     *
     * @return the mutex.
     */
    public ZkLock getEventLock() {
        return _zkEventLock;
    }

    public boolean delete(final String path) {
        try {
            retryUntilConnected(new Callable<byte[]>() {

                @Override
                public byte[] call() throws Exception {
                    _connection.delete(path);
                    return null;
                }
            });

            return true;
        } catch (ZkNoNodeException e) {
            return false;
        }
    }

    public byte[] readData(String path) {
        return readData(path, false);
    }

    public byte[] readData(String path, boolean returnNullIfPathNotExists) {
        byte[] data = null;
        try {
            data = readData(path, null);
        } catch (ZkNoNodeException e) {
            if (!returnNullIfPathNotExists) {
                throw e;
            }
        }
        return data;
    }

    public byte[] readData(String path, Stat stat) {
        return readData(path, stat, hasListeners(path));
    }

    protected byte[] readData(final String path, final Stat stat, final boolean watch) {
        byte[] data = retryUntilConnected(new Callable<byte[]>() {

            @Override
            public byte[] call() throws Exception {
                return _connection.readData(path, stat, watch);
            }
        });
        return data;
    }

    public Stat writeData(String path, byte[] object) {
        return writeData(path, object, -1);
    }

    public void cas(String path, DataUpdater updater) {
        Stat stat = new Stat();
        boolean retry;
        do {
            retry = false;
            try {
                byte[] oldData = readData(path, stat);
                byte[] newData = updater.update(oldData);
                writeData(path, newData, stat.getVersion());
            } catch (ZkBadVersionException e) {
                retry = true;
            }
        } while (retry);
    }

    public Stat writeData(final String path, final byte[] data, final int expectedVersion) {
        return retryUntilConnected(new Callable<Stat>() {

            @Override
            public Stat call() throws Exception {
                return _connection.writeData(path, data, expectedVersion);
            }
        });
    }

    public void watchForData(final String path) {
        retryUntilConnected(new Callable<Object>() {

            @Override
            public Object call() throws Exception {
                _connection.exists(path, true);
                return null;
            }
        });
    }

    public List<String> watchForChilds(final String path) {
        if (_zookeeperEventThread != null && Thread.currentThread() == _zookeeperEventThread) {
            throw new IllegalArgumentException("Must not be done in the zookeeper event thread.");
        }
        return retryUntilConnected(new Callable<List<String>>() {

            @Override
            public List<String> call() throws Exception {
                exists(path, true);
                try {
                    return getChildren(path, true);
                } catch (ZkNoNodeException e) {
                    // ignore, the "exists" watch will listen for the parent node to appear
                }
                return null;
            }
        });
    }


    public synchronized void connect(final long maxMsToWaitUntilConnected, Watcher watcher) {
        if (_eventThread != null) {
            return;
        }
        boolean started = false;
        try {
            getEventLock().lockInterruptibly();
            setShutdownTrigger(false);
            _eventThread = new ZkEventThread(_connection.getServers());
            _eventThread.start();
            _connection.connect(watcher);

            LOG.debug("Awaiting connection to Zookeeper server: " + maxMsToWaitUntilConnected);
            if (!waitUntilConnected(maxMsToWaitUntilConnected, TimeUnit.MILLISECONDS)) {
                throw new ZkTimeoutException(String.format(
                        "Unable to connect to zookeeper server[%s] within timeout %dms", _connection.getServers(), maxMsToWaitUntilConnected));
            }
            started = true;
        } catch (InterruptedException e) {
            States state = _connection.getZookeeperState();
            throw new IllegalStateException("Not connected with zookeeper server yet. Current state is " + state);
        } finally {
            getEventLock().unlock();

            // we should close the zookeeper instance, otherwise it would keep
            // on trying to connect
            if (!started) {
                close();
            }
        }
    }

    public long getCreationTime(String path) {
        try {
            getEventLock().lockInterruptibly();
            return _connection.getCreateTime(path);
        } catch (KeeperException e) {
            throw ZkException.create(e);
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    public synchronized void close() throws ZkInterruptedException {
        if (_eventThread == null) {
            return;
        }
        LOG.debug("Closing ZkClient...");
        getEventLock().lock();
        try {
            setShutdownTrigger(true);
            _currentState = null;
            _eventThread.interrupt();
            _eventThread.join(2000);
            _connection.close();
            _eventThread = null;
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
        LOG.debug("Closing ZkClient...done");
    }

    private void reconnect() {
        getEventLock().lock();
        try {
            _connection.close();
            _connection.connect(this);
        } catch (InterruptedException e) {
            throw new ZkInterruptedException(e);
        } finally {
            getEventLock().unlock();
        }
    }

    private void setShutdownTrigger(boolean triggerState) {
        _shutdownTriggered = triggerState;
    }

    private boolean getShutdownTrigger() {
        return _shutdownTriggered;
    }

    public int numberOfListeners() {
        int listeners = 0;
        for (Set<ZkChildListener> childListeners : _childListener.values()) {
            listeners += childListeners.size();
        }
        for (Set<ZkDataListener> dataListeners : _dataListener.values()) {
            listeners += dataListeners.size();
        }
        listeners += _stateListener.size();

        return listeners;
    }

    public List<?> multi(final Iterable<?> ops) {
        return retryUntilConnected(new Callable<List<?>>() {
            @Override
            public List<?> call() throws Exception {
                return _connection.multi(ops);
            }
        });
    }

    public ZooKeeper getZooKeeper() {
        return _connection != null ? _connection.getZooKeeper() : null;
    }

    public boolean isConnected() {
        return _currentState == KeeperState.SyncConnected;
    }
    
    
    /**
     * A CAS operation
     */
    interface DataUpdater {

        /**
         * Updates the current data of a znode.
         *
         * @param currentData The current contents.
         * @return the new data that should be written back to ZooKeeper.
         */
        public byte[] update(byte[] currentData);

    }
}
