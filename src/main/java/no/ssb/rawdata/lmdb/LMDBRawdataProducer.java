package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataContentNotBufferedException;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataProducer;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class LMDBRawdataProducer implements RawdataProducer {

    final int producerId;
    final String topic;
    final LMDBBackend lmdbBackend;
    final Map<String, LMDBRawdataMessage> buffer = new ConcurrentHashMap<>();
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Consumer<LMDBRawdataProducer> closeAction;

    LMDBRawdataProducer(int producerId, LMDBBackend lmdbBackend, String topic, Consumer<LMDBRawdataProducer> closeAction) {
        this.producerId = producerId;
        this.lmdbBackend = lmdbBackend;
        this.topic = topic;
        this.closeAction = closeAction;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public String lastPosition() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        return lmdbBackend.getLastPosition();
    }

    @Override
    public RawdataMessage.Builder builder() throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        return new RawdataMessage.Builder() {
            String position;
            Map<String, byte[]> data = new LinkedHashMap<>();

            @Override
            public RawdataMessage.Builder position(String position) {
                this.position = position;
                return this;
            }

            @Override
            public RawdataMessage.Builder put(String key, byte[] payload) {
                data.put(key, payload);
                return this;
            }

            @Override
            public LMDBRawdataMessage build() {
                return new LMDBRawdataMessage(position, data);
            }
        };
    }

    @Override
    public LMDBRawdataMessage buffer(RawdataMessage.Builder builder) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        return buffer(builder.build());
    }

    @Override
    public LMDBRawdataMessage buffer(RawdataMessage message) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        buffer.put(message.position(), (LMDBRawdataMessage) message);
        return (LMDBRawdataMessage) message;
    }

    @Override
    public void publish(String... positions) throws RawdataClosedException, RawdataContentNotBufferedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        for (String position : positions) {
            if (!buffer.containsKey(position)) {
                throw new RawdataContentNotBufferedException(String.format("position %s is not in buffer", position));
            }
        }
        for (String position : positions) {
            LMDBRawdataMessage payload = buffer.remove(position);
            try {
                lmdbBackend.write(payload);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
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
            closeAction.accept(this);
        }
    }
}
