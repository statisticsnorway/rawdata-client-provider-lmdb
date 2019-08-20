package no.ssb.rawdata.lmdb;

import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class DirectByteBufferPool implements AutoCloseable {

    final BlockingQueue<ByteBuffer> deque = new LinkedBlockingDeque<>();

    public DirectByteBufferPool(int n, int capacity) {
        for (int i = 0; i < n; i++) {
            try {
                deque.put(ByteBuffer.allocateDirect(capacity));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public ByteBuffer acquire() {
        try {
            return deque.take();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void release(ByteBuffer buffer) {
        try {
            deque.put(buffer.clear());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        deque.clear();
    }
}
