package no.ssb.rawdata.lmdb;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNotBufferedException;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

class LMDBRawdataProducer implements RawdataProducer {

    final int producerId;
    final String topic;
    final LMDBBackend lmdbBackend;
    final Map<String, LMDBRawdataMessage.Builder> buffer = new ConcurrentHashMap<>();
    final AtomicBoolean closed = new AtomicBoolean(false);

    final ULID ulid = new ULID();
    final AtomicReference<ULID.Value> previousIdRef = new AtomicReference<>(ulid.nextValue());

    LMDBRawdataProducer(int producerId, LMDBBackend lmdbBackend, String topic) {
        this.producerId = producerId;
        this.lmdbBackend = lmdbBackend;
        this.topic = topic;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        return new LMDBRawdataMessage.Builder();
    }

    @Override
    public RawdataProducer buffer(RawdataMessage.Builder _builder) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        LMDBRawdataMessage.Builder builder = (LMDBRawdataMessage.Builder) _builder;
        buffer.put(builder.position, builder);
        return this;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataNotBufferedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataNotBufferedException(String.format("position %s is not in buffer", position));
            }
        }
        for (String position : positions) {
            LMDBRawdataMessage.Builder builder = buffer.get(position);
            builder.ulid(getOrGenerateNextUlid(builder));
            lmdbBackend.write(builder.build());
        }
    }

    private ULID.Value getOrGenerateNextUlid(LMDBRawdataMessage.Builder builder) {
        ULID.Value id = builder.ulid;
        while (id == null) {
            ULID.Value previousUlid = previousIdRef.get();
            ULID.Value attemptedId = RawdataProducer.nextMonotonicUlid(this.ulid, previousUlid);
            if (previousIdRef.compareAndSet(previousUlid, attemptedId)) {
                id = attemptedId;
            }
        }
        return id;
    }

    @Override
    public CompletableFuture<Void> publishAsync(String... positions) {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        return CompletableFuture.runAsync(() -> publish(positions));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LMDBRawdataProducer that = (LMDBRawdataProducer) o;
        return producerId == that.producerId &&
                topic.equals(that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(producerId, topic);
    }

    @Override
    public String toString() {
        return "LMDBRawdataProducer{" +
                "producerId=" + producerId +
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
