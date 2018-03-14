package com.github.bingoohuang.mtcp.util;

import lombok.Getter;
import lombok.Setter;
import lombok.val;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import static com.github.bingoohuang.mtcp.util.BagEntry.State.*;
import static java.util.concurrent.atomic.AtomicIntegerFieldUpdater.newUpdater;

public class BagEntry {
    private static final AtomicIntegerFieldUpdater<BagEntry> stateUpdater
            = newUpdater(BagEntry.class, "state");
    private volatile int state;

    @Getter @Setter private volatile String tenantId;

    interface State {
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
        return stateUpdater.compareAndSet(this, STATE_FREE, STATE_USING);
    }

    public final boolean stateUsingToRemoved() {
        return stateUpdater.compareAndSet(this, STATE_USING, STATE_REMOVED);
    }

    public final boolean stateReservedToRemoved() {
        return stateUpdater.compareAndSet(this, STATE_RESERVED, STATE_REMOVED);
    }

    public final boolean stateFreeToReserved() {
        return stateUpdater.compareAndSet(this, STATE_FREE, STATE_RESERVED);
    }

    public final boolean stateReservedToFree() {
        return stateUpdater.compareAndSet(this, STATE_RESERVED, STATE_FREE);
    }

    public final void stateToFree() {
        setState(STATE_FREE);
    }

    public final boolean isStateFree() {
        return getState() == STATE_FREE;
    }

    public final boolean isStateUsing() {
        return getState() == STATE_USING;
    }

    public final String stateToString() {
        val currentState = this.state;
        switch (currentState) {
            case STATE_USING:
                return "USING";
            case STATE_FREE:
                return "FREE";
            case STATE_REMOVED:
                return "REMOVED";
            case STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid:" + currentState;
        }
    }

}
