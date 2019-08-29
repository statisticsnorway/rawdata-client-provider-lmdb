package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class LMDBRawdataConsumer implements RawdataConsumer {

    static final Logger log = LoggerFactory.getLogger(LMDBRawdataConsumer.class);

    final int consumerId;
    final Lock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();
    final String topic;
    final LMDBBackend lmdbBackend;
    final AtomicReference<String> positionRef;
    final AtomicBoolean closed = new AtomicBoolean(false);

    public LMDBRawdataConsumer(int consumerId, LMDBBackend lmdbBackend, String topic, String initialPosition) {
        this.consumerId = consumerId;
        this.lmdbBackend = lmdbBackend;
        this.topic = topic;
        this.positionRef = new AtomicReference<>(initialPosition);
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public LMDBRawdataMessage receive(int timeout, TimeUnit unit) throws InterruptedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("consumer for is closed, topic: %s", topic));
        }
        lock.lockInterruptibly();
        try {
            int pollIntervalNanos = 250 * 1000 * 1000;
            long expireTimeNano = System.nanoTime() + unit.toNanos(timeout);
            while (true) {
                String initialPosition = positionRef.get();
                List<LMDBRawdataMessage> contents = lmdbBackend.readContentBulk(initialPosition, false, 1);
                if (contents.size() == 1) {
                    positionRef.compareAndSet(initialPosition, contents.get(0).position());
                    return contents.get(0);
                }
                if (contents.size() > 1) {
                    log.error("BUG: Got size > 1 from readContentBulk, using the first element as fallback");
                    positionRef.compareAndSet(initialPosition, contents.get(0).position());
                    return contents.get(0);
                }
                // end of stream, wait until expire time for another message to appear
                long durationNano = expireTimeNano - System.nanoTime();
                if (durationNano <= 0) {
                    return null; // timeout
                }
                condition.await(Math.min(durationNano, pollIntervalNanos), TimeUnit.NANOSECONDS);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public CompletableFuture<LMDBRawdataMessage> receiveAsync() {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("consumer for is closed, topic: %s", topic));
        }
        return CompletableFuture.supplyAsync(() -> {
            try {
                return receive(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void seek(long timestamp) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LMDBRawdataConsumer that = (LMDBRawdataConsumer) o;
        return consumerId == that.consumerId &&
                topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(consumerId, topic);
    }

    @Override
    public String toString() {
        return "LMDBRawdataConsumer{" +
                "consumerId=" + consumerId +
                ", topic='" + topic + '\'' +
                '}';
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            lmdbBackend.close();
        }
    }
}
