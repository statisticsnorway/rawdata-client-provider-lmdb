package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;

class LMDBRawdataClient implements RawdataClient {

    final Path folder;
    final AtomicBoolean closed = new AtomicBoolean(false);
    final Map<String, LMDBRawdataProducer> producers = new ConcurrentSkipListMap<>();
    final Map<String, LMDBRawdataConsumer> consumers = new ConcurrentSkipListMap<>();
    final Map<String, LMDBBackend> backendByTopic = new ConcurrentHashMap<>();

    LMDBRawdataClient(String folder) {
        this.folder = Paths.get(folder);
    }

    @Override
    public LMDBRawdataProducer producer(String topic) {
        LMDBBackend lmdbBackend = backendByTopic.computeIfAbsent(topic, t -> new LMDBBackend(folder.resolve(t), 1, 10));
        LMDBRawdataProducer producer = new LMDBRawdataProducer(lmdbBackend, topic, p -> {
            producers.remove(p.topic());
            if (!consumers.containsKey(p.topic())) {
                LMDBBackend be = backendByTopic.remove(p.topic());
                be.close();
            } else {
                toString();
            }
        });
        this.producers.put(topic, producer);
        return producer;
    }

    @Override
    public RawdataConsumer consumer(String topic, String initialPosition) {
        LMDBRawdataConsumer consumer;
        LMDBBackend lmdbBackend = backendByTopic.computeIfAbsent(topic, t -> new LMDBBackend(folder.resolve(t), 1, 10));
        consumer = new LMDBRawdataConsumer(lmdbBackend, topic, initialPosition, c -> {
            consumers.remove(c.topic());
            if (!producers.containsKey(c.topic())) {
                LMDBBackend be = backendByTopic.remove(c.topic());
                be.close();
            } else {
                toString();
            }
        });
        consumers.put(topic, consumer);
        return consumer;
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            for (LMDBRawdataProducer producer : producers.values()) {
                try {
                    producer.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
            producers.clear();
            for (LMDBRawdataConsumer consumer : consumers.values()) {
                try {
                    consumer.close();
                } catch (Throwable t) {
                    // ignore
                }
            }
            consumers.clear();
            for (LMDBBackend lmdbBackend : backendByTopic.values()) {
                lmdbBackend.close();
            }
            backendByTopic.clear();
        }
    }
}
