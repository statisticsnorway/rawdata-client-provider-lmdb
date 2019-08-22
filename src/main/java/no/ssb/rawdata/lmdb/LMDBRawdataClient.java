package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Optional.ofNullable;

class LMDBRawdataClient implements RawdataClient {

    final AtomicInteger producerAndConsumerIdGenerator = new AtomicInteger();
    final Path folder;
    final AtomicBoolean closed = new AtomicBoolean(false);

    final Map<String, LMDBBackend> backendByTopic = new LinkedHashMap<>();
    final List<LMDBRawdataProducer> producers = new CopyOnWriteArrayList<>();
    final List<LMDBRawdataConsumer> consumers = new CopyOnWriteArrayList<>();

    LMDBRawdataClient(String folder) {
        this.folder = Paths.get(folder);
    }

    @Override
    public LMDBRawdataProducer producer(String topic) {
        LMDBBackend lmdbBackend = openLmdbBackendAndIncreaseReferenceCount(topic);
        LMDBRawdataProducer producer = new LMDBRawdataProducer(producerAndConsumerIdGenerator.incrementAndGet(), lmdbBackend, topic);
        producers.add(producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, String initialPosition) {
        LMDBBackend lmdbBackend = openLmdbBackendAndIncreaseReferenceCount(topic);
        LMDBRawdataConsumer consumer = new LMDBRawdataConsumer(producerAndConsumerIdGenerator.incrementAndGet(), lmdbBackend, topic, initialPosition);
        consumers.add(consumer);
        return consumer;
    }

    private LMDBBackend openLmdbBackendAndIncreaseReferenceCount(String topic) {
        return ofNullable(backendByTopic.computeIfPresent(topic, (t, b) -> {
            if (b.incrementRefCount()) {
                return b;
            }
            return createLMDBBackendAndIncrementRefCount(t); // remap to
        })).orElseGet(() -> backendByTopic.computeIfAbsent(topic, t -> createLMDBBackendAndIncrementRefCount(t)));
    }

    private LMDBBackend createLMDBBackendAndIncrementRefCount(String t) {
        LMDBBackend newLmdbBackend = new LMDBBackend(folder.resolve(t), 1, 10);
        newLmdbBackend.incrementRefCount();
        return newLmdbBackend;
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
