package no.ssb.rawdata.lmdb;

import de.huxhorn.sulky.ulid.ULID;
import no.ssb.rawdata.api.RawdataMessage;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Optional.ofNullable;


/**
 * This classes encapsulates modelling and methods needed in Lightning Memory-Mapped Database (LMDB) to build rawdata
 * producers and consumers.
 * <p><p>
 * The model:
 * Instances of this class represent a single "stream" and are tied to a specific file-system directory
 * (path) at construction time which is where the corresponding LMDB data and lock files live.
 * <p><p>
 * This stream uses 3 databases (ordered key-value stores) internally to represent the rawdata sequence and
 * content:
 * <p>
 * <ul>
 * <li>data
 * <ul>Key<li>ULID. offset: 0, length: 16 bytes, encoding: ULID.Value binary encoding</li>
 * <li>Name, offset: 16, length: variable, encoding: UTF-8</li></ul>
 * <ul>Value<li>byte[]. offset: 0, length: variable, encoding: binary</li></ul>
 * </li>
 * <p>
 * <li>sequence</li>
 * <ul>Key<li>ULID. offset: 0, length: 16 bytes, encoding: ULID.Value binary encoding</li></ul>
 * <ul>Value<li>length(position) (1 byte), position (UTF-8), length(orderingGroup) (1 byte), orderingGroup (UTF-8), sequenceNumber (8 bytes, long)</li></ul>
 * <p>
 * <li>index</li>
 * <ul>Key<li>Position. offset: 0, length: variable, encoding: UTF-8</li></ul>
 * <ul>Value<li>ULID. offset: 0, length: 16 bytes, encoding: ULID.Value binary encoding</li></ul>
 * </ul>
 * <p>
 */
class LMDBBackend extends JVMSuppressIllegalAccess implements AutoCloseable {

    static class SequenceRecord {
        final ULID.Value ulid;
        final String position;
        final String orderingGroup;
        final long sequenceNumber;

        SequenceRecord(ULID.Value ulid, String position, String orderingGroup, long sequenceNumber) {
            this.ulid = ulid;
            this.position = position;
            this.orderingGroup = orderingGroup;
            this.sequenceNumber = sequenceNumber;
        }
    }

    static final Logger log = LoggerFactory.getLogger(LMDBBackend.class);

    final Env<ByteBuffer> env;
    final Dbi<ByteBuffer> data; // ordered by ulid
    final Dbi<ByteBuffer> sequence; // ordered by ulid
    final Dbi<ByteBuffer> index; // ordered by position

    final Path path;
    final DirectByteBufferPool contentDataBufferPool;
    final DirectByteBufferPool keyBufferPool;

    final ReferenceCounter referenceCounter = new ReferenceCounter();

    /**
     * @param path                      the folder where this LMDB backend should access the database and lock files.
     * @param mapSize                   the size of the memory-mapped database (in bytes). The database file will be
     *                                  allocated to this size.
     * @param writeConcurrency          maximum number of concurrent writers, used to allocate enough direct
     *                                  byte-buffers in pools to avoid deadlock.
     * @param readConcurrency           maximum number of concurrent readers, used to allocate enough direct
     *                                  byte-buffers in pools to avoid deadlock.
     * @param maxMessageFileContentSize max-size (in bytes) of a single content element in any message written to the
     *                                  database using this instance.
     */
    LMDBBackend(Path path, long mapSize, int writeConcurrency, int readConcurrency, int maxMessageFileContentSize) {
        log.trace("{} {} -- constructor path={}, mapSize={}, writeConcurrency={}, readConcurrency={}, maxMessageFileContentSize={}", toString(), path.toString(), path.toAbsolutePath(), mapSize, writeConcurrency, readConcurrency, maxMessageFileContentSize);
        this.path = path;
        contentDataBufferPool = new DirectByteBufferPool(writeConcurrency, maxMessageFileContentSize);
        keyBufferPool = new DirectByteBufferPool((2 * writeConcurrency) + readConcurrency, 512);
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        env = Env.create()
                .setMaxDbs(3)
                .setMapSize(mapSize)
                .open(path.toFile());
        data = env.openDbi("data", DbiFlags.MDB_CREATE);
        sequence = env.openDbi("sequence", DbiFlags.MDB_CREATE);
        index = env.openDbi("index", DbiFlags.MDB_CREATE);
    }

    /**
     * Drop all 3 databases. All data will be erased.
     */
    public void drop() {
        log.trace("{} {} -- constructor", toString(), path.toString());
        try (Txn<ByteBuffer> txn = env.txnWrite()) {
            sequence.drop(txn);
            index.drop(txn);
            data.drop(txn);
            txn.commit();
        }
    }

    /**
     * Append a rawdata-message to the end of this stream.
     *
     * @param message the message to append
     */
    public void write(LMDBRawdataMessage message) {
        log.trace("{} {} -- write {}", toString(), path.toString(), message);
        ByteBuffer keyBuffer = keyBufferPool.acquire();
        try {
            keyBuffer.put(message.ulid().toBytes());
            int mark = keyBuffer.position();
            try (Txn<ByteBuffer> txn = env.txnWrite()) {
                if (postionToULIDBytes(txn, message.position()) != null) {
                    throw new IllegalArgumentException("Illegal write, position already exists in database: " + message.position());
                }
                ByteBuffer messageDataBuffer = contentDataBufferPool.acquire();
                try {
                    for (Map.Entry<String, byte[]> entry : message.data.entrySet()) {
                        keyBuffer.put(entry.getKey().getBytes(StandardCharsets.UTF_8));
                        messageDataBuffer.clear();
                        messageDataBuffer.put(entry.getValue());
                        data.put(txn, keyBuffer.flip(), messageDataBuffer.flip());
                        keyBuffer.clear();
                        keyBuffer.position(mark);
                    }
                } finally {
                    contentDataBufferPool.release(messageDataBuffer);
                }
                keyBuffer.limit(keyBuffer.position());
                keyBuffer.flip();
                ByteBuffer positionBuffer = keyBufferPool.acquire();
                try {
                    byte[] positionBytes = message.position().getBytes(StandardCharsets.UTF_8);
                    byte[] orderingGroupBytes = ofNullable(message.orderingGroup()).map(og -> og.getBytes(StandardCharsets.UTF_8)).orElse(new byte[0]);
                    if (2 + positionBytes.length + orderingGroupBytes.length + 4 > 511) {
                        throw new RuntimeException("");
                    }
                    positionBuffer
                            .put((byte) positionBytes.length)
                            .put(positionBytes)
                            .put((byte) orderingGroupBytes.length)
                            .put(orderingGroupBytes)
                            .putLong(message.sequenceNumber()).flip();
                    sequence.put(txn, keyBuffer, positionBuffer);
                    index.put(txn, positionBuffer.clear().put(positionBytes).flip(), keyBuffer);
                } finally {
                    keyBufferPool.release(positionBuffer);
                }
                txn.commit();
            }
        } finally {
            keyBufferPool.release(keyBuffer);
        }
    }

    /**
     * Get the current last message in the stream as given by the sequence database.
     *
     * @return the current last message in the stream, or null if the stream is empty.
     */
    public RawdataMessage getLastMessage() {
        log.trace("{} {} -- getLastMessage", toString(), path.toString());
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return firstMessageInRange(txn, KeyRange.allBackward());
        }
    }

    private RawdataMessage firstMessageInRange(Txn<ByteBuffer> txn, KeyRange<ByteBuffer> range) {
        try (CursorIterator<ByteBuffer> iterator = sequence.iterate(txn, range)) {
            if (!iterator.hasNext()) {
                return null; // sequence empty
            }
            CursorIterator.KeyVal<ByteBuffer> keyVal = iterator.next();
            SequenceRecord sequenceRecord = readSequenceRecord(keyVal.key(), keyVal.val());
            return readContentInRange(txn, Map.of(sequenceRecord.ulid, sequenceRecord), range, 1).stream().findFirst().orElse(null);
        }
    }

    private NavigableMap<ULID.Value, SequenceRecord> doGetSequence(Txn<ByteBuffer> txn, KeyRange<ByteBuffer> range, int n) {
        try (CursorIterator<ByteBuffer> iterator = sequence.iterate(txn, range)) {
            NavigableMap<ULID.Value, SequenceRecord> map = new TreeMap<>();
            for (int i = 0; i < n && iterator.hasNext(); i++) {
                CursorIterator.KeyVal<ByteBuffer> keyVal = iterator.next();
                SequenceRecord sequenceRecord = readSequenceRecord(keyVal.key(), keyVal.val());
                map.put(sequenceRecord.ulid, sequenceRecord);
            }
            return map;
        }
    }

    private SequenceRecord readSequenceRecord(ByteBuffer keyBuffer, ByteBuffer valueBuffer) {
        ULID.Value key = bytesToULID(keyBuffer);
        int positionLength = valueBuffer.get();
        byte[] positionBuf = readBytes(valueBuffer, positionLength);
        String position = new String(positionBuf, StandardCharsets.UTF_8);
        int orderingGroupLength = valueBuffer.get();
        byte[] orderingGroupBuf = readBytes(valueBuffer, orderingGroupLength);
        String orderingGroup = orderingGroupBuf.length == 0 ? null : new String(orderingGroupBuf, StandardCharsets.UTF_8);
        long sequenceNumber = valueBuffer.getLong();
        return new SequenceRecord(key, position, orderingGroup, sequenceNumber);
    }

    /**
     * Get the ulid-value belonging to a given position in the stream.
     *
     * @param position the position to get the ulid-key of
     * @return the ulid value at the given position, or null if no such position exists in the stream.
     */
    public ULID.Value ulidOf(String position) {
        log.trace("{} {} -- ulidOf {}", toString(), path.toString(), position);
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer dataKey = postionToULIDBytes(txn, position);
            if (dataKey == null) {
                return null; // not found
            }
            return bytesToULID(dataKey);
        }
    }

    /**
     * Get a sequence of messages with at most 'n' elements start at the given initial-position. The message sequence is
     * decided by the 'data' database which uses the same ordering as the 'sequence' database.
     *
     * @param cursor the cursor defining the range of keys from which to start returning content.
     * @param n      a maximum limit of how many positions to include in the returned message sequece. This value must
     *               be less than or equal to 10^6.
     * @return the message-sequence, or an empty sequence if the initial-position does not exists in the stream.
     */
    public List<LMDBRawdataMessage> readContentBulk(LMDBCursor cursor, int n) {
        log.trace("{} {} -- readContentBulk {}, {}", toString(), path.toString(), cursor, n);
        if (n > 1000000) {
            throw new IllegalArgumentException("Not allowed: n > 1_000_000");
        }
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer startKeyBytes = keyBufferPool.acquire();
            try {
                KeyRange<ByteBuffer> range = cursor.toKeyRange(startKeyBytes);
                NavigableMap<ULID.Value, SequenceRecord> positionByUlid = doGetSequence(txn, range, n);
                if (positionByUlid.isEmpty()) {
                    return Collections.emptyList();
                }
                startKeyBytes.clear().put(positionByUlid.firstKey().toBytes()).flip();
                return readContentInRange(txn, positionByUlid, KeyRange.atLeast(startKeyBytes), n);

            } finally {
                keyBufferPool.release(startKeyBytes);
            }
        }
    }

    private List<LMDBRawdataMessage> readContentInRange(Txn<ByteBuffer> txn, Map<ULID.Value, SequenceRecord> sequenceValueByUlid, KeyRange<ByteBuffer> range, int n) {
        try (CursorIterator<ByteBuffer> iterator = data.iterate(txn, range)) {
            List<LMDBRawdataMessage> result = new ArrayList<>(n);
            Map<String, byte[]> messageData = null;
            byte[] prevKeyBytes = new byte[16];
            ULID.Value prevKey = null;
            int i = -1;
            while (iterator.hasNext()) {
                CursorIterator.KeyVal<ByteBuffer> keyVal = iterator.next();
                byte[] keyBytes = readBytes(keyVal.key(), 16);
                ULID.Value key = ULID.fromBytes(keyBytes);
                if (!Arrays.equals(prevKeyBytes, keyBytes)) {
                    if (prevKey != null) {
                        SequenceRecord sequenceValue = sequenceValueByUlid.get(prevKey);
                        result.add(new LMDBRawdataMessage(prevKey, sequenceValue.orderingGroup, sequenceValue.sequenceNumber, sequenceValue.position, messageData));
                    }
                    if (++i >= n) {
                        return result; // reached limit
                    }
                    messageData = new LinkedHashMap<>();
                    prevKeyBytes = keyBytes;
                    prevKey = key;
                }
                String name = contentNameOf(keyVal.key());
                byte[] value = contentValueOf(keyVal.val());
                messageData.put(name, value);
            }
            if (prevKey != null) {
                SequenceRecord sequenceValue = sequenceValueByUlid.get(prevKey);
                result.add(new LMDBRawdataMessage(prevKey, sequenceValue.orderingGroup, sequenceValue.sequenceNumber, sequenceValue.position, messageData));
            }
            return result; // reached end of stream
        }
    }

    private String contentNameOf(ByteBuffer keyBuffer) {
        byte[] buf = new byte[keyBuffer.limit() - 16];
        keyBuffer.get(buf);
        String name = new String(buf, StandardCharsets.UTF_8);
        return name;
    }

    private byte[] contentValueOf(ByteBuffer valBuffer) {
        byte[] value = new byte[valBuffer.remaining()];
        valBuffer.get(value);
        return value;
    }

    private byte[] readBytes(ByteBuffer input, int n) {
        byte[] buf = new byte[Math.min(input.remaining(), n)];
        input.get(buf);
        return buf;
    }

    /**
     * Look up a position in the sequence and find the corresponding data key.
     *
     * @param txn
     * @param position
     * @return
     */
    private ByteBuffer postionToULIDBytes(Txn<ByteBuffer> txn, String position) {
        ByteBuffer buffer = keyBufferPool.acquire();
        try {
            buffer.put(position.getBytes(StandardCharsets.UTF_8));
            ByteBuffer ulidBytes = index.get(txn, buffer.flip());
            return ulidBytes;
        } finally {
            keyBufferPool.release(buffer);
        }
    }

    private ULID.Value bytesToULID(ByteBuffer byteBuffer) {
        byte[] buf = new byte[16];
        byteBuffer.get(buf);
        return ULID.fromBytes(buf);
    }

    /**
     * Increment internal reference count used to know whether backend can be closed or not when close method is called.
     *
     * @return true if the refCount was incremented, otherwise false (the backend was already closed).
     */
    public boolean incrementRefCount() {
        log.trace("{} {} -- incrementRefCount", toString(), path.toString());
        return referenceCounter.incrementRefCount();
    }

    @Override
    public void close() {
        log.trace("{} {} -- close", toString(), path.toString());
        if (referenceCounter.decrementRefCount()) {
            keyBufferPool.close();
            contentDataBufferPool.close();
            data.close();
            sequence.close();
            index.close();
            env.close();
        }
    }

    public boolean isClosed() {
        log.trace("{} {} -- isClosed", toString(), path.toString());
        return env.isClosed();
    }
}
