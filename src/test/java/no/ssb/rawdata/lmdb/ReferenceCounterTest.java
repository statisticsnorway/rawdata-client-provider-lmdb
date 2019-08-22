package no.ssb.rawdata.lmdb;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class ReferenceCounterTest {

    @Test
    public void thatReferenceCounterStartsAtMinus() {
        ReferenceCounter counter = new ReferenceCounter();
        assertEquals(counter.getRefCount(), -1);
    }

    @Test
    public void thatDecrementFirstSetsCounterToZeroAndReturnsTrue() {
        ReferenceCounter counter = new ReferenceCounter();
        assertTrue(counter.decrementRefCount());
        assertEquals(counter.getRefCount(), 0);
    }

    @Test
    public void thatCounterCannotBeChangedAfterReachingZeroAndThatIncrementAndDecrementThenAlwaysReturnsFalse() {
        ReferenceCounter counter = new ReferenceCounter();
        counter.decrementRefCount(); // sets counter to 0
        assertEquals(counter.getRefCount(), 0);
        counter.incrementRefCount();
        assertEquals(counter.getRefCount(), 0);
        counter.decrementRefCount();
        assertEquals(counter.getRefCount(), 0);
    }

    @Test
    public void thatFirstIncrementReturnsTrue() {
        ReferenceCounter counter = new ReferenceCounter();
        assertTrue(counter.incrementRefCount());
    }

    @Test
    public void thatFirstIncrementSetsCounterToOne() {
        ReferenceCounter counter = new ReferenceCounter();
        counter.incrementRefCount();
        assertEquals(counter.getRefCount(), 1);
    }

    @Test
    public void thatReferenceCounterIncreasesOneForEachIncrementAfterFirstIncrement() {
        ReferenceCounter counter = new ReferenceCounter();
        counter.incrementRefCount();
        assertEquals(counter.getRefCount(), 1);
        for (int i = 1; i <= 10; i++) {
            counter.incrementRefCount();
            assertEquals(counter.getRefCount(), 1 + i);
        }
    }

    @Test
    public void thatReferenceCounterCanBeIncreasedAndThenDecreasedAgainBackToZeroAndStayStableAtZero() {
        ReferenceCounter counter = new ReferenceCounter();
        assertTrue(counter.incrementRefCount());
        assertEquals(counter.getRefCount(), 1);
        assertTrue(counter.incrementRefCount());
        assertEquals(counter.getRefCount(), 2);
        assertTrue(counter.incrementRefCount());
        assertEquals(counter.getRefCount(), 3);
        assertFalse(counter.decrementRefCount());
        assertEquals(counter.getRefCount(), 2);
        assertFalse(counter.decrementRefCount());
        assertEquals(counter.getRefCount(), 1);
        assertTrue(counter.incrementRefCount());
        assertEquals(counter.getRefCount(), 2);
        assertFalse(counter.decrementRefCount());
        assertEquals(counter.getRefCount(), 1);
        assertTrue(counter.decrementRefCount());
        assertEquals(counter.getRefCount(), 0);
        assertFalse(counter.decrementRefCount());
        assertEquals(counter.getRefCount(), 0);
        assertFalse(counter.incrementRefCount());
        assertEquals(counter.getRefCount(), 0);
    }
}