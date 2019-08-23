package no.ssb.rawdata.lmdb;

import no.ssb.rawdata.api.RawdataClient;
import no.ssb.rawdata.api.RawdataClientInitializer;
import no.ssb.service.provider.api.ProviderName;

import java.util.Map;
import java.util.Set;

@ProviderName("lmdb")
public class LMDBRawdataClientInitializer implements RawdataClientInitializer {

    @Override
    public String providerId() {
        return "lmdb";
    }

    @Override
    public Set<String> configurationKeys() {
        return Set.of(
                "lmdb.folder",
                "lmdb.map-size",
                "lmdb.message.file.max-size",
                "lmdb.topic.write-concurrency",
                "lmdb.topic.read-concurrency"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String folder = configuration.get("lmdb.folder");
        long lmdbMapSize = Long.parseLong(configuration.get("lmdb.map-size"));
        int maxMessageContentFileSize = Integer.parseInt(configuration.get("lmdb.message.file.max-size"));
        int writeConcurrencyPerTopic = Integer.parseInt(configuration.get("lmdb.topic.write-concurrency"));
        int readConcurrencyPerTopic = Integer.parseInt(configuration.get("lmdb.topic.read-concurrency"));
        return new LMDBRawdataClient(folder, lmdbMapSize, maxMessageContentFileSize, writeConcurrencyPerTopic, readConcurrencyPerTopic);
    }
}
