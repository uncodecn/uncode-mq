package cn.uncode.mq.zk;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ZkLock extends ReentrantLock {

    private static final long serialVersionUID = 1L;

    private final Condition _dataChangedCondition = newCondition();
    private final Condition _stateChangedCondition = newCondition();
    private final Condition _zNodeEventCondition = newCondition();

    /**
     * This condition will be signaled if a zookeeper event was processed and the event contains a data/child change.
     *
     * @return the condition.
     */
    public Condition getDataChangedCondition() {
        return _dataChangedCondition;
    }

    /**
     * This condition will be signaled if a zookeeper event was processed and the event contains a state change
     * (connected, disconnected, session expired, etc ...).
     *
     * @return the condition.
     */
    public Condition getStateChangedCondition() {
        return _stateChangedCondition;
    }

    /**
     * This condition will be signaled if any znode related zookeeper event was received.
     *
     * @return the condition.
     */
    public Condition getZNodeEventCondition() {
        return _zNodeEventCondition;
    }
}