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
                "lmdb.folder"
        );
    }

    @Override
    public RawdataClient initialize(Map<String, String> configuration) {
        String folder = configuration.get("lmdb.folder");
        return new LMDBRawdataClient(folder);
    }
}
