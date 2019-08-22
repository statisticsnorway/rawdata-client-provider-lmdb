package no.ssb.rawdata.lmdb;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reference-counter starts at -1. The first increment operation increments the counter to 1, then subsequent
 * increment operations will increment the counter by 1. If the counter is 0 before
 * incremented by 1 on every increment operation after that. Decrement operations decrement the counter by 1, except
 * when the counter reaches 0, then the counter will never be changed again. If the counter is decremented without any
 * prior increments, the counter will be set to 0 and kept there. This class is typically used to keep track
 * of how many references are kept to a given instance, and when the count reaches 0 the resources related to the
 * reference may be released.
 */
public class ReferenceCounter {

    final AtomicInteger refCount = new AtomicInteger(-1);

    public int getRefCount() {
        return refCount.get();
    }

    /**
     * @return true if the reference count was incremented due to this operation, otherwise false (if the reference
     * count is 0)
     */
    public boolean incrementRefCount() {
        int count;
        int incr;
        do {
            count = refCount.get();
            if (count == -1) {
                incr = 2;
            } else if (count == 0) {
                return false; // already closed
            } else {
                incr = 1;
            }
        } while (!refCount.compareAndSet(count, count + incr));
        return true;
    }

    /**
     * @return true if this decrement operation caused the reference count to reach 0, false otherwise
     */
    public boolean decrementRefCount() {
        int count;
        int incr;
        do {
            count = refCount.get();
            if (count == -1) {
                incr = 1;
            } else if (count == 0) {
                return false; // already closed
            } else {
                incr = -1;
            }
        }
        while (!refCount.compareAndSet(count, count + incr));
        return (count + incr) == 0;
    }

}
