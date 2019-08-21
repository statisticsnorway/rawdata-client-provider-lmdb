package no.ssb.rawdata.lmdb;

import de.huxhorn.sulky.ulid.ULID;
import org.lmdbjava.CursorIterator;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.KeyRange;
import org.lmdbjava.Txn;

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
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;


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
 * <ul>Value<li>Position. offset: 0, length: variable, encoding: UTF-8</li></ul>
 * <p>
 * <li>index</li>
 * <ul>Key<li>Position. offset: 0, length: variable, encoding: UTF-8</li></ul>
 * <ul>Value<li>ULID. offset: 0, length: 16 bytes, encoding: ULID.Value binary encoding</li></ul>
 * </ul>
 * <p>
 */
public class LMDBBackend extends JVMSuppressIllegalAccess implements AutoCloseable {

    final ULID ulid = new ULID();
    final Env<ByteBuffer> env;
    final Dbi<ByteBuffer> data; // ordered by ulid
    final Dbi<ByteBuffer> sequence; // ordered by ulid
    final Dbi<ByteBuffer> index; // ordered by position

    final AtomicReference<ULID.Value> previousIdRef = new AtomicReference<>(ulid.nextValue());

    final Semaphore writeSemaphore;

    final DirectByteBufferPool bufferPool;
    final DirectByteBufferPool ulidBytesOnlyPool;

    public LMDBBackend(Path path, int writeConcurrency, int readConcurrency) {
        writeSemaphore = new Semaphore(writeConcurrency);
        bufferPool = new DirectByteBufferPool(2 * writeConcurrency, 1024);
        ulidBytesOnlyPool = new DirectByteBufferPool(2 * readConcurrency, 16);
        try {
            Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        env = Env.create()
                .setMaxDbs(3)
                .setMapSize(1 * 1024 * 1024)
                .open(path.toFile(), EnvFlags.MDB_WRITEMAP);
        data = env.openDbi("data", DbiFlags.MDB_CREATE);
        sequence = env.openDbi("sequence", DbiFlags.MDB_CREATE);
        index = env.openDbi("index", DbiFlags.MDB_CREATE);
    }

    /**
     * Drop all 3 databases. All data will be erased.
     */
    public void drop() {
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
     * @throws InterruptedException if the calling thread is interrupted while waiting on internal resources
     */
    public void write(LMDBRawdataMessage message) throws InterruptedException {
        /*
         * Use semaphore to avoid deadlock due to concurrent nested ByteBuffer acquire from pool.
         */
        writeSemaphore.acquire();
        try {
            ULID.Value id = null;
            do {
                ULID.Value previousUlid = previousIdRef.get();
                ULID.Value attemptedId = ulid.nextStrictlyMonotonicValue(previousUlid).orElseThrow();
                if (previousIdRef.compareAndSet(previousUlid, attemptedId)) {
                    id = attemptedId;
                }
            } while (id == null);
            ByteBuffer keyBuffer = bufferPool.acquire();
            try {
                keyBuffer.put(id.toBytes());
                int mark = keyBuffer.position();
                try (Txn<ByteBuffer> txn = env.txnWrite()) {
                    ByteBuffer messageDataBuffer = bufferPool.acquire();
                    try {
                        for (Map.Entry<String, byte[]> entry : message.getData().entrySet()) {
                            keyBuffer.put(entry.getKey().getBytes(StandardCharsets.UTF_8));
                            messageDataBuffer.clear();
                            messageDataBuffer.put(entry.getValue());
                            data.put(txn, keyBuffer.flip(), messageDataBuffer.flip());
                            keyBuffer.clear();
                            keyBuffer.position(mark);
                        }
                    } finally {
                        bufferPool.release(messageDataBuffer);
                    }
                    keyBuffer.limit(keyBuffer.position());
                    keyBuffer.flip();
                    ByteBuffer positionBuffer = bufferPool.acquire();
                    try {
                        positionBuffer.put(message.position().getBytes(StandardCharsets.UTF_8));
                        positionBuffer.flip();
                        sequence.put(txn, keyBuffer, positionBuffer);
                        index.put(txn, positionBuffer, keyBuffer);
                    } finally {
                        bufferPool.release(positionBuffer);
                    }
                    txn.commit();
                }
            } finally {
                bufferPool.release(keyBuffer);
            }
        } finally {
            writeSemaphore.release();
        }
    }

    /**
     * Get the first position in the stream as given by the sequence database.
     *
     * @return the first position in the stream, or null if the stream is empty.
     */
    public String getFirstPosition() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return firstPositionInRange(txn, KeyRange.all());
        }
    }

    /**
     * Get the current last position in the stream as given by the sequence database.
     *
     * @return the current last position in the stream, or null if the stream is empty.
     */
    public String getLastPosition() {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return firstPositionInRange(txn, KeyRange.allBackward());
        }
    }

    private String firstPositionInRange(Txn<ByteBuffer> txn, KeyRange<ByteBuffer> range) {
        try (CursorIterator<ByteBuffer> iterator = sequence.iterate(txn, range)) {
            if (!iterator.hasNext()) {
                return null; // sequence empty
            }
            CursorIterator.KeyVal<ByteBuffer> keyVal = iterator.next();
            ByteBuffer positionBytes = keyVal.val();
            byte[] buf = readBytes(positionBytes, positionBytes.remaining());
            return new String(buf, StandardCharsets.UTF_8);
        }
    }

    /**
     * Get a sequence of positions with at most 'n' elements start at the given initial-position. The sequence is
     * decided by the 'sequence' database. If the
     *
     * @param initialPosition the initial-position to mark the beginning of the sequence
     * @param inclusive       whether or not to include the initial-position in the returned sequence.
     * @param n               a maximum limit of how many positions to include in the returned sequence. This value must
     *                        be less than or equal to 10^6.
     * @return the sequence of positions, or an empty sequence if the initial-position does not exists in the stream.
     */
    public List<String> getSequence(String initialPosition, boolean inclusive, int n) {
        if (n > 1000000) {
            throw new IllegalArgumentException("Not allowed: n > 10^6");
        }
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            return doGetSequence(txn, initialPosition, inclusive, n).stream().map(e -> e.position).collect(Collectors.toList());
        }
    }

    private List<ULIDAndPosition> doGetSequence(Txn<ByteBuffer> txn, String initialPosition, boolean inclusive, int n) {
        KeyRange<ByteBuffer> range;
        if (initialPosition == null) {
            range = KeyRange.all();
        } else {
            ByteBuffer startKeyBytes = postionToULIDBytes(txn, initialPosition);
            if (startKeyBytes == null) {
                return Collections.emptyList(); // initial-position not found
            }
            if (inclusive) {
                range = KeyRange.atLeast(startKeyBytes);
            } else {
                range = KeyRange.greaterThan(startKeyBytes);
            }
        }
        try (CursorIterator<ByteBuffer> iterator = sequence.iterate(txn, range)) {
            List<ULIDAndPosition> sequence = new ArrayList<>(n);
            for (int i = 0; i < n && iterator.hasNext(); i++) {
                CursorIterator.KeyVal<ByteBuffer> keyVal = iterator.next();
                ByteBuffer positionBytes = keyVal.val();
                byte[] buf = readBytes(positionBytes, positionBytes.remaining());
                sequence.add(new ULIDAndPosition(bytesToULID(keyVal.key()), new String(buf, StandardCharsets.UTF_8)));
            }
            return sequence;
        }
    }

    /**
     * Get the message belonging to a given random position in the stream.
     *
     * @param position the position from which to get the message
     * @return the message at the given position, or null if no such position exists in the stream.
     */
    public LMDBRawdataMessage readContentOf(String position) {
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            ByteBuffer dataKey = postionToULIDBytes(txn, position);
            if (dataKey == null) {
                return null; // not found
            }
            return getDataContent(txn, position, dataKey);
        }
    }

    /**
     * Get a sequence of messages with at most 'n' elements start at the given initial-position. The message sequence is
     * decided by the 'data' database which uses the same ordering as the 'sequence' database.
     *
     * @param initialPosition the initial-position to mark the beginning of the message sequence
     * @param inclusive       whether or not to include the initial-position in the returned message sequence.
     * @param n               a maximum limit of how many positions to include in the returned message sequece. This value must
     *                        be less than or equal to 10^6.
     * @return the message-sequence, or an empty sequence if the initial-position does not exists in the stream.
     */
    public List<LMDBRawdataMessage> readContentBulk(String initialPosition, boolean inclusive, int n) {
        if (n > 1000000) {
            throw new IllegalArgumentException("Not allowed: n > 1_000_000");
        }
        try (Txn<ByteBuffer> txn = env.txnRead()) {
            Map<ULID.Value, String> positionByUlid;
            if (initialPosition == null) {
                initialPosition = firstPositionInRange(txn, KeyRange.all());
                if (initialPosition == null) {
                    return Collections.emptyList();
                }
                inclusive = true;
            }
            List<ULIDAndPosition> ulidAndPositionSequence = doGetSequence(txn, initialPosition, inclusive, n);
            if (ulidAndPositionSequence.isEmpty()) {
                return Collections.emptyList();
            }
            positionByUlid = ulidAndPositionSequence.stream().collect(Collectors.toMap(e -> e.key, e -> e.position));
            KeyRange<ByteBuffer> range;
            ByteBuffer startKeyBytes = ulidBytesOnlyPool.acquire();
            try {
                startKeyBytes.put(ulidAndPositionSequence.get(0).key.toBytes()).flip();
                range = KeyRange.atLeast(startKeyBytes);
                return readContentInRange(txn, positionByUlid, range, n);
            } finally {
                ulidBytesOnlyPool.release(startKeyBytes);
            }
        }
    }

    /**
     * Get message belonging to the given ULID.
     *
     * @param txn
     * @param ulidBytes a byte representation of the key prefix in the stream
     * @return
     */
    private LMDBRawdataMessage getDataContent(Txn<ByteBuffer> txn, String position, ByteBuffer ulidBytes) {
        ULID.Value ulid = bytesToULID(ulidBytes);
        ULID.Value upperBoundValue = this.ulid.nextStrictlyMonotonicValue(ulid, ulid.timestamp()).orElseThrow();
        ulidBytes.flip();
        byte[] upperBoundByteBuffer = upperBoundValue.toBytes();
        ByteBuffer upperBound = ulidBytesOnlyPool.acquire();
        try {
            upperBound.put(upperBoundByteBuffer);
            KeyRange<ByteBuffer> range = KeyRange.closedOpen(ulidBytes, upperBound.flip());
            List<LMDBRawdataMessage> messages = readContentInRange(txn, Map.of(ulid, position), range, 1);
            if (messages.isEmpty()) {
                return null;
            }
            return messages.get(0);
        } finally {
            ulidBytesOnlyPool.release(upperBound);
        }
    }

    private List<LMDBRawdataMessage> readContentInRange(Txn<ByteBuffer> txn, Map<ULID.Value, String> positionByUlid, KeyRange<ByteBuffer> range, int n) {
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
                        result.add(new LMDBRawdataMessage(positionByUlid.get(prevKey), messageData));
                    }
                    if (++i >= n) {
                        return result; // reached limit
                    }
                    messageData = new LinkedHashMap<>();
                    prevKeyBytes = keyBytes;
                    prevKey = key;
                }
                String name = contentNameOf(keyVal);
                byte[] value = contentValueOf(keyVal);
                messageData.put(name, value);
            }
            if (prevKey != null) {
                result.add(new LMDBRawdataMessage(positionByUlid.get(prevKey), messageData));
            }
            return result; // reached end of stream
        }
    }

    private String contentNameOf(CursorIterator.KeyVal<ByteBuffer> keyVal) {
        byte[] buf = new byte[keyVal.key().limit() - 16];
        keyVal.key().getLong();
        keyVal.key().getLong();
        keyVal.key().get(buf);
        String name = new String(buf, StandardCharsets.UTF_8);
        return name;
    }

    private byte[] contentValueOf(CursorIterator.KeyVal<ByteBuffer> keyVal) {
        byte[] value = new byte[keyVal.val().remaining()];
        keyVal.val().get(value);
        return value;
    }

    private byte[] readBytes(ByteBuffer input, int n) {
        int mark = input.position();
        byte[] buf = new byte[Math.min(input.remaining(), n)];
        input.get(buf);
        input.position(mark);
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
        ByteBuffer buffer = bufferPool.acquire();
        try {
            buffer.put(position.getBytes(StandardCharsets.UTF_8));
            ByteBuffer ulidBytes = index.get(txn, buffer.flip());
            return ulidBytes;
        } finally {
            bufferPool.release(buffer);
        }
    }

    private ULID.Value bytesToULID(ByteBuffer byteBuffer) {
        byte[] buf = new byte[16];
        byteBuffer.get(buf);
        return ULID.fromBytes(buf);
    }

    @Override
    public void close() {
        data.close();
        sequence.close();
        index.close();
        env.close();
    }

    public boolean isClosed() {
        return env.isClosed();
    }
}
