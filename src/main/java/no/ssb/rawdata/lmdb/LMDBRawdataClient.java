package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class LMDBRawdataClient implements RawdataClient {

    static final Logger log = LoggerFactory.getLogger(LMDBRawdataClient.class);

    final AtomicInteger producerAndConsumerIdGenerator = new AtomicInteger();
    final Path folder;
    final AtomicBoolean closed = new AtomicBoolean(false);

    final Lock mutex = new ReentrantLock();
    final Map<String, Set<LMDBRawdataProducer>> producers = new LinkedHashMap<>();
    final Map<String, Set<LMDBRawdataConsumer>> consumers = new LinkedHashMap<>();
    final Map<String, LMDBBackend> backendByTopic = new LinkedHashMap<>();

    LMDBRawdataClient(String folder) {
        this.folder = Paths.get(folder);
    }

    @Override
    public LMDBRawdataProducer producer(String topic) {
        try {
            mutex.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            LMDBBackend lmdbBackend = backendByTopic.computeIfAbsent(topic, t -> new LMDBBackend(folder.resolve(t), 1, 10));
            LMDBRawdataProducer producer = new LMDBRawdataProducer(producerAndConsumerIdGenerator.incrementAndGet(), lmdbBackend, topic, p -> {
                try {
                    mutex.lockInterruptibly();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    Set<LMDBRawdataProducer> producerSet = producers.get(topic);
                    producerSet.remove(p);
                    Set<LMDBRawdataConsumer> consumerSet = consumers.get(p.topic());
                    if (producerSet.isEmpty() && (consumerSet == null || consumerSet.isEmpty())) {
                        LMDBBackend be = backendByTopic.remove(p.topic());
                        be.close();
                    }
                } finally {
                    mutex.unlock();
                }
            });
            this.producers.computeIfAbsent(topic, t -> new LinkedHashSet<>()).add(producer);
            return producer;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public RawdataConsumer consumer(String topic, String initialPosition) {
        try {
            mutex.lockInterruptibly();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try {
            LMDBRawdataConsumer consumer;
            LMDBBackend lmdbBackend = backendByTopic.computeIfAbsent(topic, t -> new LMDBBackend(folder.resolve(t), 1, 10));
            consumer = new LMDBRawdataConsumer(producerAndConsumerIdGenerator.incrementAndGet(), lmdbBackend, topic, initialPosition, c -> {
                try {
                    mutex.lockInterruptibly();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {
                    Set<LMDBRawdataConsumer> consumerSet = consumers.get(c.topic());
                    consumerSet.remove(c);
                    Set<LMDBRawdataProducer> producerSet = producers.get(topic);
                    if (consumerSet.isEmpty() && (producerSet == null || producerSet.isEmpty())) {
                        LMDBBackend be = backendByTopic.remove(c.topic());
                        be.close();
                    }
                } finally {
                    mutex.unlock();
                }
            });
            consumers.computeIfAbsent(topic, t -> new LinkedHashSet<>()).add(consumer);
            return consumer;
        } finally {
            mutex.unlock();
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public void close() {
        // copy maps and sets to avoid concurrent-modification exceptions due to the closeAction collection manipulation
        if (closed.compareAndSet(false, true)) {
            for (Set<LMDBRawdataProducer> producers : new LinkedHashMap<>(producers).values()) {
                for (LMDBRawdataProducer producer : new LinkedHashSet<>(producers)) {
                    try {
                        producer.close();
                    } catch (Throwable t) {
                        log.error("While attempting to close producer: " + producer.toString(), t);
                    }
                }
            }
            producers.clear();
            for (Set<LMDBRawdataConsumer> consumers : new LinkedHashMap<>(consumers).values()) {
                for (LMDBRawdataConsumer consumer : new LinkedHashSet<>(consumers)) {
                    try {
                        consumer.close();
                    } catch (Throwable t) {
                        log.error("While attempting to close consumer: " + consumer.toString(), t);
                    }
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
