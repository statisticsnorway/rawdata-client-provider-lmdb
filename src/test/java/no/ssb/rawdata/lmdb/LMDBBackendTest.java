package no.ssb.rawdata.lmdb;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class LMDBBackendTest {

    LMDBBackend lmdbBackend;

    @BeforeMethod
    public void setup() {
        lmdbBackend = new LMDBBackend(Paths.get("target/lmdbwritertest"), 1, 1);
        lmdbBackend.drop();
    }

    @AfterMethod
    public void teardown() {
        lmdbBackend.close();
        lmdbBackend = null;
    }

    @Test
    public void thatDataIsRetainedAcrossBackendInstances() throws InterruptedException {
        lmdbBackend.write(new LMDBRawdataMessage("a", Map.of("payload", "hello".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.close();
        lmdbBackend = null;
        lmdbBackend = new LMDBBackend(Paths.get("target/lmdbwritertest"), 1, 1);
        LMDBRawdataMessage message = lmdbBackend.readContentOf("a");
        assertEquals(message.data.size(), 1);
        assertTrue(message.data.containsKey("payload"));
        assertEquals(new String(message.get("payload"), StandardCharsets.UTF_8), "hello");
    }

    @Test
    public void thatSingleItemCanBeWrittenAndRead() throws InterruptedException {
        lmdbBackend.write(new LMDBRawdataMessage("a", Map.of("payload", "hello".getBytes(StandardCharsets.UTF_8))));
        LMDBRawdataMessage message = lmdbBackend.readContentOf("a");
        assertEquals(message.data.size(), 1);
        assertTrue(message.data.containsKey("payload"));
        assertEquals(new String(message.get("payload"), StandardCharsets.UTF_8), "hello");
    }

    @Test
    public void thatSingleItemWithMultipleContentsCanBeWrittenAndRead() throws InterruptedException {
        lmdbBackend.write(new LMDBRawdataMessage("a", Map.of(
                "payload1", "hello 1".getBytes(StandardCharsets.UTF_8),
                "payload2", "hello 2".getBytes(StandardCharsets.UTF_8),
                "payload3", "hello 3".getBytes(StandardCharsets.UTF_8)
        )));
        LMDBRawdataMessage message = lmdbBackend.readContentOf("a");
        assertEquals(message.getData().size(), 3);
        assertTrue(message.getData().containsKey("payload1"));
        assertEquals(new String(message.get("payload1"), StandardCharsets.UTF_8), "hello 1");
        assertTrue(message.getData().containsKey("payload3"));
        assertEquals(new String(message.get("payload2"), StandardCharsets.UTF_8), "hello 2");
        assertTrue(message.getData().containsKey("payload2"));
        assertEquals(new String(message.get("payload3"), StandardCharsets.UTF_8), "hello 3");
    }

    @Test
    public void thatGetFirstPositionWorks() throws InterruptedException {
        lmdbBackend.write(new LMDBRawdataMessage("b", Map.of("payload", "hello B".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("a", Map.of("payload", "hello A".getBytes(StandardCharsets.UTF_8))));
        assertEquals(lmdbBackend.getFirstPosition(), "b");
    }

    @Test
    public void thatGetLastPositionWorks() throws InterruptedException {
        lmdbBackend.write(new LMDBRawdataMessage("b", Map.of("payload", "hello B".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("a", Map.of("payload", "hello A".getBytes(StandardCharsets.UTF_8))));
        assertEquals(lmdbBackend.getLastPosition(), "a");
    }

    @Test
    public void thatGetSequenceVariantsAreCorrect() throws InterruptedException {
        lmdbBackend.write(new LMDBRawdataMessage("b", Map.of("payload", "hello B".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("a", Map.of("payload", "hello A".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("f", Map.of("payload", "hello F".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("c", Map.of("payload", "hello C".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("d", Map.of("payload", "hello D".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("x", Map.of("payload", "hello X".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("s", Map.of("payload", "hello S".getBytes(StandardCharsets.UTF_8))));
        lmdbBackend.write(new LMDBRawdataMessage("_", Map.of("payload", "hello _".getBytes(StandardCharsets.UTF_8))));
        assertEquals(lmdbBackend.getSequence("b", true, 10), List.of("b", "a", "f", "c", "d", "x", "s", "_"));
        assertEquals(lmdbBackend.getSequence("b", true, 8), List.of("b", "a", "f", "c", "d", "x", "s", "_"));
        assertEquals(lmdbBackend.getSequence("b", true, 7), List.of("b", "a", "f", "c", "d", "x", "s"));
        assertEquals(lmdbBackend.getSequence("b", false, 10), List.of("a", "f", "c", "d", "x", "s", "_"));
        assertEquals(lmdbBackend.getSequence("b", false, 7), List.of("a", "f", "c", "d", "x", "s", "_"));
        assertEquals(lmdbBackend.getSequence("b", false, 6), List.of("a", "f", "c", "d", "x", "s"));
        assertEquals(lmdbBackend.getSequence("f", true, 2), List.of("f", "c"));
        assertEquals(lmdbBackend.getSequence("f", false, 3), List.of("c", "d", "x"));
    }

    @Test
    public void thatReadContentBulkVariantsAreCorrect() throws InterruptedException {
        LMDBRawdataMessage expectedB = new LMDBRawdataMessage("b", Map.of("payload", "hello B".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedB);
        LMDBRawdataMessage expectedA = new LMDBRawdataMessage("a", Map.of("payload", "hello A".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedA);
        LMDBRawdataMessage expectedF = new LMDBRawdataMessage("f", Map.of("payload", "hello F".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedF);
        LMDBRawdataMessage expectedC = new LMDBRawdataMessage("c", Map.of("payload1", "hello C 1".getBytes(StandardCharsets.UTF_8),
                "payload2", "hello C 2".getBytes(StandardCharsets.UTF_8),
                "payload3", "hello C 3".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedC);
        LMDBRawdataMessage expectedD = new LMDBRawdataMessage("d", Map.of("payload", "hello D".getBytes(StandardCharsets.UTF_8),
                "payload2", "hello D 2".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedD);
        LMDBRawdataMessage expectedX = new LMDBRawdataMessage("x", Map.of("payload", "hello X".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedX);
        LMDBRawdataMessage expectedS = new LMDBRawdataMessage("s", Map.of("payload", "hello S".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expectedS);
        LMDBRawdataMessage expected_ = new LMDBRawdataMessage("_", Map.of("payload", "hello _".getBytes(StandardCharsets.UTF_8)));
        lmdbBackend.write(expected_);
        assertEquals(lmdbBackend.readContentBulk("b", true, 10), List.of(expectedB, expectedA, expectedF, expectedC, expectedD, expectedX, expectedS, expected_));
        assertEquals(lmdbBackend.readContentBulk("b", true, 8), List.of(expectedB, expectedA, expectedF, expectedC, expectedD, expectedX, expectedS, expected_));
        assertEquals(lmdbBackend.readContentBulk("b", true, 7), List.of(expectedB, expectedA, expectedF, expectedC, expectedD, expectedX, expectedS));
        assertEquals(lmdbBackend.readContentBulk("b", false, 10), List.of(expectedA, expectedF, expectedC, expectedD, expectedX, expectedS, expected_));
        assertEquals(lmdbBackend.readContentBulk("b", false, 7), List.of(expectedA, expectedF, expectedC, expectedD, expectedX, expectedS, expected_));
        assertEquals(lmdbBackend.readContentBulk("b", false, 6), List.of(expectedA, expectedF, expectedC, expectedD, expectedX, expectedS));
        assertEquals(lmdbBackend.readContentBulk("f", true, 2), List.of(expectedF, expectedC));
        assertEquals(lmdbBackend.readContentBulk("f", false, 3), List.of(expectedC, expectedD, expectedX));
    }
}
