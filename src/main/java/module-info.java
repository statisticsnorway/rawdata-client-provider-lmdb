import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.rawdata.lmdb.LMDBRawdataClientInitializer;

module no.ssb.rawdata.lmdb {
    requires no.ssb.rawdata.api;
    requires no.ssb.service.provider.api;
    requires de.huxhorn.sulky.ulid;
    requires lmdbjava;
    requires org.slf4j;

    provides RawdataClientInitializer with LMDBRawdataClientInitializer;
}
