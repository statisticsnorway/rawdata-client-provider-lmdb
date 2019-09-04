package no.ssb.rawdata.lmdb;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClosedException;
import no.ssb.rawdata.api.RawdataConsumer;
import no.ssb.rawdata.api.RawdataCursor;
import no.ssb.rawdata.api.RawdataMessage;
import no.ssb.rawdata.api.RawdataNoSuchPositionException;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Optional.ofNullable;

class LMDBRawdataClient implements RawdataClient {

    final Path folder;
    final long lmdbMapSize;
    final int maxMessageContentFileSize;
    final int writeConcurrencyPerTopic;
    final int readConcurrencyPerTopic;

    final AtomicInteger producerAndConsumerIdGenerator = new AtomicInteger();
    final AtomicBoolean closed = new AtomicBoolean(false);

    final Map<String, LMDBBackend> backendByTopic = new LinkedHashMap<>();
    final List<LMDBRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<LMDBRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    LMDBRawdataClient(String folder, long lmdbMapSize, int maxMessageContentFileSize, int writeConcurrencyPerTopic, int readConcurrencyPerTopic) {
        this.folder = Paths.get(folder);
        this.lmdbMapSize = lmdbMapSize;
        this.maxMessageContentFileSize = maxMessageContentFileSize;
        this.writeConcurrencyPerTopic = writeConcurrencyPerTopic;
        this.readConcurrencyPerTopic = readConcurrencyPerTopic;
    }

    @Override
    public LMDBRawdataProducer producer(String topic) {
        LMDBBackend lmdbBackend = openLmdbBackendAndIncreaseReferenceCount(topic);
        LMDBRawdataProducer producer = new LMDBRawdataProducer(producerAndConsumerIdGenerator.incrementAndGet(), lmdbBackend, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, RawdataCursor cursor) {
        LMDBBackend lmdbBackend = openLmdbBackendAndIncreaseReferenceCount(topic);
        LMDBRawdataConsumer consumer = new LMDBRawdataConsumer(producerAndConsumerIdGenerator.incrementAndGet(), lmdbBackend, topic, (LMDBCursor) cursor);
        consumers.add(consumer);
        return consumer;
    }

    @Override
    public RawdataCursor cursorOf(String topic, ULID.Value ulid, boolean inclusive) {
        return new LMDBCursor(ulid, inclusive, true);
    }

    @Override
    public RawdataCursor cursorOf(String topic, String position, boolean inclusive, long approxTimestamp, Duration tolerance) {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        LMDBBackend lmdbBackend = openLmdbBackendAndIncreaseReferenceCount(topic);
        try {
            ULID.Value ulidValue = lmdbBackend.ulidOf(position);
            if (ulidValue == null) {
                throw new RawdataNoSuchPositionException("Position not found: " + position);
            }
            return new LMDBCursor(ulidValue, inclusive, true);
        } finally {
            lmdbBackend.close();
        }
    }

    @Override
    public RawdataMessage lastMessage(String topic) throws RawdataClosedException {
        if (isClosed()) {
            throw new RawdataClosedException(String.format("producer for is closed, topic: %s", topic));
        }
        LMDBBackend lmdbBackend = openLmdbBackendAndIncreaseReferenceCount(topic);
        try {
            return lmdbBackend.getLastMessage();
        } finally {
            lmdbBackend.close();
        }
    }

    private LMDBBackend openLmdbBackendAndIncreaseReferenceCount(String topic) {
        return ofNullable(backendByTopic.computeIfPresent(topic, (t, b) -> {
            if (b.incrementRefCount()) {
                return b;
            }
            return createLMDBBackendAndIncrementRefCount(t); // remap to
        })).orElseGet(() -> backendByTopic.computeIfAbsent(topic, t -> createLMDBBackendAndIncrementRefCount(t)));
    }

    private LMDBBackend createLMDBBackendAndIncrementRefCount(String topic) {
        LMDBBackend newLmdbBackend = createNewLmdbBackend(topic);
        newLmdbBackend.incrementRefCount();
        return newLmdbBackend;
    }

    private LMDBBackend createNewLmdbBackend(String topic) {
        return new LMDBBackend(folder.resolve(topic), lmdbMapSize, writeConcurrencyPerTopic, readConcurrencyPerTopic, maxMessageContentFileSize);
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        // copy maps and sets to avoid concurrent-modification exceptions due to the closeAction collection manipulation
        if (closed.compareAndSet(false, true)) {
            for (LMDBRawdataProducer producer : producers) {
                producer.close();
            }
            producers.clear();
            for (LMDBRawdataConsumer consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
            backendByTopic.clear();
        }
    }
}
