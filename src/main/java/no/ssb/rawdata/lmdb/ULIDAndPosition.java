package no.ssb.rawdata.lmdb;

import de.huxhorn.sulky.ulid.ULID;

class ULIDAndPosition {
    final ULID.Value key;
    final String position;

    ULIDAndPosition(ULID.Value key, String position) {
        this.key = key;
        this.position = position;
    }
}
