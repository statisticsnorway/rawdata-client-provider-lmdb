package no.ssb.rawdata.lmdb;

import de.huxhorn.sulky.ulid.ULID;
import org.lmdbjava.KeyRange;

import java.nio.ByteBuffer;
import java.util.Objects;

class LMDBCursor {

    /**
     * Need not exactly match an existing ulid-value.
     */
    final ULID.Value startKey;

    /**
     * Whether or not to include the element with ulid-value matching the lower-bound exactly.
     */
    final boolean inclusive;

    /**
     * Traversal direction, true signifies forward.
     */
    final boolean forward;

    LMDBCursor(ULID.Value startKey, boolean inclusive, boolean forward) {
        this.startKey = startKey;
        this.inclusive = inclusive;
        this.forward = forward;
    }

    KeyRange<ByteBuffer> toKeyRange(ByteBuffer startKeyBuffer) {
        startKeyBuffer.put(startKey.toBytes()).flip();
        if (forward) {
            if (inclusive) {
                return KeyRange.atLeast(startKeyBuffer);
            } else {
                return KeyRange.greaterThan(startKeyBuffer);
            }
        } else {
            if (inclusive) {
                return KeyRange.atLeastBackward(startKeyBuffer);
            } else {
                return KeyRange.greaterThanBackward(startKeyBuffer);
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LMDBCursor that = (LMDBCursor) o;
        return inclusive == that.inclusive &&
                forward == that.forward &&
                startKey.equals(that.startKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, inclusive, forward);
    }

    @Override
    public String toString() {
        return "LMDBCursor{" +
                "startKey=" + startKey +
                ", inclusive=" + inclusive +
                ", forward=" + forward +
                '}';
    }
}
