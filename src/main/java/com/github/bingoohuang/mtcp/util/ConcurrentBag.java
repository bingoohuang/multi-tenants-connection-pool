package com.github.bingoohuang.mtcp.util;

import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.github.bingoohuang.mtcp.util.ClockSource.currentTime;
import static com.github.bingoohuang.mtcp.util.ClockSource.elapsedNanos;
import static java.lang.Thread.yield;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 * <p>
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @param <T> the templated type to store in the bag
 * @author Brett Wooldridge
 */
@Slf4j
public class ConcurrentBag<T extends BagEntry> implements AutoCloseable {
    private final CopyOnWriteArrayList<T> sharedList;

    private final ThreadLocalList<T> threadLocalList;
    private final BagStateListener listener;
    private final AtomicInteger waiters;

    private volatile boolean closed;

    private final SynchronousQueue<T> handoffQueue;

    public interface BagStateListener {
        void addBagItem(int waiting);
    }

    /**
     * Construct a ConcurrentBag with the specified listener.
     *
     * @param listener the BagStateListener to attach to this bag
     */
    public ConcurrentBag(final BagStateListener listener) {
        this.listener = listener;

        this.handoffQueue = new SynchronousQueue<>(true);
        this.waiters = new AtomicInteger();
        this.sharedList = new CopyOnWriteArrayList<>();
        this.threadLocalList = new ThreadLocalList<>();
    }

    /**
     * The method will borrow a BagEntry from the bag, blocking for the
     * specified timeout if none are available.
     *
     * @param timeout  how long to wait before giving up, in units of unit
     * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
     * @return a borrowed instance from the bag or null if a timeout occurs
     * @throws InterruptedException if interrupted while waiting
     */
    public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException {
        // Try the thread-local list first
        val entry = threadLocalList.get();
        if (entry != null) {
            return entry;
        }

        // Otherwise, scan the shared list ... then poll the handoff queue
        val bagWaiting = waiters.incrementAndGet();
        try {
            for (val bagEntry : sharedList) {
                if (bagEntry.stateFreeToUsing()) {
                    // If we may have stolen another waiter's connection, request another bag add.
                    if (bagWaiting > 1) {
                        listener.addBagItem(bagWaiting - 1);
                    }

                    return bagEntry;
                }
            }

            listener.addBagItem(bagWaiting);

            timeout = timeUnit.toNanos(timeout);
            do {
                val start = currentTime();
                val bagEntry = handoffQueue.poll(timeout, NANOSECONDS);
                if (bagEntry == null) {
                    return bagEntry;
                }

                if (bagEntry.stateFreeToUsing()) {
                    return bagEntry;
                }

                timeout -= elapsedNanos(start);
            } while (timeout > 10_000);

            return null;
        } finally {
            waiters.decrementAndGet();
        }
    }

    /**
     * This method will return a borrowed object to the bag.  Objects
     * that are borrowed from the bag but never "requited" will result
     * in a memory leak.
     *
     * @param bagEntry the value to return to the bag
     * @throws NullPointerException  if value is null
     * @throws IllegalStateException if the bagEntry was not borrowed from the bag
     */
    public void requite(final T bagEntry) {
        bagEntry.stateToFree();

        for (int i = 0; waiters.get() > 0; i++) {
            if (!bagEntry.isStateFree() || handoffQueue.offer(bagEntry)) {
                return;
            } else if ((i & 0xff) == 0xff) {
                parkNanos(MICROSECONDS.toNanos(10));
            } else {
                yield();
            }
        }

        threadLocalList.add(bagEntry);
    }

    /**
     * Add a new object to the bag for others to borrow.
     *
     * @param bagEntry an object to add to the bag
     */
    public void add(final T bagEntry) {
        if (closed) {
            log.info("ConcurrentBag has been closed, ignoring add()");
            throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
        }

        sharedList.add(bagEntry);

        // spin until a thread takes it or none are waiting
        while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            yield();
        }
    }

    /**
     * Remove a value from the bag.  This method should only be called
     * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
     *
     * @param bagEntry the value to remove
     * @return true if the entry was removed, false otherwise
     * @throws IllegalStateException if an attempt is made to remove an object
     *                               from the bag that was not borrowed or reserved first
     */
    public boolean remove(final T bagEntry) {
        if (!bagEntry.stateUsingToRemoved() && !bagEntry.stateReservedToRemoved() && !closed) {
            log.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
            return false;
        }

        val removed = sharedList.remove(bagEntry);
        if (!removed && !closed) {
            log.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
        }

        return removed;
    }

    /**
     * Close the bag to further adds.
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * This method provides a "snapshot" in time of the BagEntry
     * items in the bag in the specified state.  It does not "lock"
     * or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in list before performing any action on them.
     *
     * @param state one of the {@link BagEntry} states
     * @return a possibly empty list of objects having the state specified
     */
    public List<T> values(final int state) {
        val list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    public List<T> valuesUsing() {
        val list = sharedList.stream().filter(e -> e.isStateUsing()).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    public List<T> valuesFree() {
        val list = sharedList.stream().filter(e -> e.isStateFree()).collect(Collectors.toList());
        Collections.reverse(list);
        return list;
    }

    /**
     * This method provides a "snapshot" in time of the bag items.  It
     * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
     * on items in the list, or understand the concurrency implications of
     * modifying items, before performing any action on them.
     *
     * @return a possibly empty list of (all) bag items
     */
    @SuppressWarnings("unchecked")
    public List<T> values() {
        return (List<T>) sharedList.clone();
    }

    /**
     * The method is used to make an item in the bag "unavailable" for
     * borrowing.  It is primarily used when wanting to operate on items
     * returned by the <code>values(int)</code> method.  Items that are
     * reserved can be removed from the bag via <code>remove(T)</code>
     * without the need to unreserve them.  Items that are not removed
     * from the bag can be make available for borrowing again by calling
     * the <code>unreserve(T)</code> method.
     *
     * @param bagEntry the item to reserve
     * @return true if the item was able to be reserved, false otherwise
     */
    public boolean reserve(final T bagEntry) {
        return bagEntry.stateFreeToReserved();
    }

    /**
     * This method is used to make an item reserved via <code>reserve(T)</code>
     * available again for borrowing.
     *
     * @param bagEntry the item to unreserve
     */
    public void unreserve(final T bagEntry) {
        if (bagEntry.stateReservedToFree()) {
            // spin until a thread takes it or none are waiting
            while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
                yield();
            }
        } else {
            log.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
        }
    }

    /**
     * Get the number of threads pending (waiting) for an item from the
     * bag to become available.
     *
     * @return the number of threads waiting for items from the bag
     */
    public int getWaitingThreadCount() {
        return waiters.get();
    }


    public int countStateUsing() {
        int count = 0;
        for (val e : sharedList) {
            if (e.isStateUsing()) {
                count++;
            }
        }
        return count;
    }


    public int countStateFree() {
        int count = 0;
        for (val e : sharedList) {
            if (e.isStateFree()) {
                count++;
            }
        }
        return count;
    }

    /**
     * Get a count of the number of items in the specified state at the time of this call.
     *
     * @param state the state of the items to count
     * @return a count of how many items in the bag are in the specified state
     */
    public int getCount(final int state) {
        int count = 0;
        for (val e : sharedList) {
            if (e.getState() == state) {
                count++;
            }
        }
        return count;
    }

    public int[] getStateCounts() {
        final int[] states = new int[6];
        for (val e : sharedList) {
            ++states[e.getState()];
        }
        states[4] = sharedList.size();
        states[5] = waiters.get();

        return states;
    }

    /**
     * Get the total number of items in the bag.
     *
     * @return the number of items in the bag
     */
    public int size() {
        return sharedList.size();
    }

    public void dumpState() {
        sharedList.forEach(entry -> log.info(entry.toString()));
    }

}
