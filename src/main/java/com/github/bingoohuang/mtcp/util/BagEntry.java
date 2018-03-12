package com.github.bingoohuang.mtcp.util;

public abstract class BagEntry {
    interface State {
        int STATE_FREE = 0;
        int STATE_USING = 1;
        int STATE_REMOVED = -1;
        int STATE_RESERVED = -2;
    }

    protected abstract boolean compareAndSet(int expectState, int newState);

    public abstract void setState(int newState);

    public abstract int getState();

    public boolean stateFreeToUsing() {
        return compareAndSet(State.STATE_FREE, State.STATE_USING);
    }

    public boolean stateUsingToRemoved() {
        return compareAndSet(State.STATE_USING, State.STATE_REMOVED);
    }

    public boolean stateReservedToRemoved() {
        return compareAndSet(State.STATE_RESERVED, State.STATE_REMOVED);
    }

    public boolean stateFreeToReserved() {
        return compareAndSet(State.STATE_FREE, State.STATE_RESERVED);
    }

    public boolean stateReservedToFree() {
        return compareAndSet(State.STATE_RESERVED, State.STATE_FREE);
    }

    public String stateToString() {
        switch (getState()) {
            case State.STATE_USING:
                return "USING";
            case State.STATE_FREE:
                return "FREE";
            case State.STATE_REMOVED:
                return "REMOVED";
            case State.STATE_RESERVED:
                return "RESERVED";
            default:
                return "Invalid";
        }
    }

    public void stateToFree() {
        setState(State.STATE_FREE);
    }

    public boolean isStateFree() {
        return getState() == State.STATE_FREE;
    }

    public boolean isStateUsing() {
        return getState() == State.STATE_USING;
    }
}
