package com.github.bingoohuang.mtcp.util;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class BagEntry {
    private static final AtomicIntegerFieldUpdater<BagEntry> stateUpdater = newUpdater(BagEntry.class, "state");
    private volatile int state;

    @Getter @Setter private volatile String tenantId;

    public BagEntry() {
    }

    private interface State {
        int STATE_FREE = 0;
        int STATE_USING = 1;
        int STATE_REMOVED = -1;
        int STATE_RESERVED = -2;
    }

    public int getState() {
        return stateUpdater.get(this);
    }

    private void setState(int newState) {
        stateUpdater.set(this, newState);
    }

    public final boolean stateFreeToUsing() {
        return stateUpdater.compareAndSet(this, State.STATE_FREE, State.STATE_USING);
    }

    public final boolean stateUsingToRemoved() {
        return stateUpdater.compareAndSet(this, State.STATE_USING, State.STATE_REMOVED);
    }

    public final boolean stateReservedToRemoved() {
        return stateUpdater.compareAndSet(this, State.STATE_RESERVED, State.STATE_REMOVED);
    }

    public final boolean stateFreeToReserved() {
        return stateUpdater.compareAndSet(this, State.STATE_FREE, State.STATE_RESERVED);
    }

    public final boolean stateReservedToFree() {
        return stateUpdater.compareAndSet(this, State.STATE_RESERVED, State.STATE_FREE);
    }

    public final void stateToFree() {
        setState(State.STATE_FREE);
        setTenantId(null);
    }

    public final boolean isStateFree() {
        return getState() == State.STATE_FREE;
    }

    public final boolean isStateUsing() {
        return getState() == State.STATE_USING;
    }

    public final String stateToString() {
        val currentState = this.state;
        switch (currentState) {
            case State.STATE_USING:
                return "USING";
            case State.STATE_FREE:
                return "FREE";
            case State.STATE_REMOVED:
                return "REMOVED";
            case State.STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid:" + currentState;
        }
    }

}
