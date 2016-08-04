package cn.uncode.mq.zk;

import java.util.List;


/**
 * An {@link ZkChildListener} can be registered at a {@link ZkClient} for listening on zk child changes for a given
 * path.
 * <p/>
 * Node: Also this listener re-subscribes it watch for the path on each zk event (zk watches are one-timers) is is not
 * guaranteed that events on the path are missing (see http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html#ch_zkWatches). An
 * implementation of this class should take that into account.
 */
public interface ZkChildListener {

    /**
     * Called when the children of the given path changed.
     * <p>
     * NOTICE: if subscribing a node(not exists), the event will be triggered while the node(parent) were created.
     * </p>
     *
     * @param parentPath      The parent path
     * @param currentChildren The children or null if the root node (parent path) was deleted.
     * @throws Exception
     */
    public void handleChildChange(String parentPath, List<String> currentChildren) throws Exception;
}
