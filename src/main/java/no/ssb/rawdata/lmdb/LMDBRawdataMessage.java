package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataMessage;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class LMDBRawdataMessage implements RawdataMessage {

    String position;
    Map<String, byte[]> data;

    public LMDBRawdataMessage() {
    }

    public LMDBRawdataMessage(String position, Map<String, byte[]> data) {
        if (position == null) {
            throw new IllegalArgumentException("position cannot be null");
        }
        if (data == null) {
            throw new IllegalArgumentException("data cannot be null");
        }
        this.position = position;
        this.data = data;
    }

    @Override
    public String position() {
        return position;
    }

    @Override
    public Set<String> keys() {
        return data.keySet();
    }

    @Override
    public byte[] get(String key) {
        return data.get(key);
    }

    public String getPosition() {
        return position;
    }

    public void setPosition(String position) {
        this.position = position;
    }

    public Map<String, byte[]> getData() {
        return data;
    }

    public void setData(Map<String, byte[]> data) {
        this.data = data;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LMDBRawdataMessage that = (LMDBRawdataMessage) o;
        return Objects.equals(position, that.position) &&
                allArraysEquals(that);
    }

    private boolean allArraysEquals(LMDBRawdataMessage that) {
        for (Map.Entry<String, byte[]> entry : data.entrySet()) {
            if (!that.data.containsKey(entry.getKey())) {
                return false;
            }
            if (!Arrays.equals(entry.getValue(), that.data.get(entry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(position, data);
    }

    @Override
    public String toString() {
        return "PulsarRawdataMessage{" +
                "position='" + position + '\'' +
                '}';
    }
}
