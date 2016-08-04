package cn.uncode.mq.zk;


/**
 * An {@link ZkDataListener} can be registered at a {@link ZkClient} for listening on zk data changes for a given path.
 * <p/>
 * Node: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not
 * guaranteed that events on the path are missing (see http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkWatches). An
 * implementation of this class should take that into account.
 */
public interface ZkDataListener {

    public void handleDataChange(String dataPath, byte[] data) throws Exception;

    public void handleDataDeleted(String dataPath) throws Exception;
}