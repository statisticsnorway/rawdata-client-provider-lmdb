package no.ssb.rawdata.lmdb;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class LMDBBackendTest {

    LMDBBackend lmdbBackend;

    @BeforeMethod
    public void setup() {
        lmdbBackend = new LMDBBackend(Paths.get("target/lmdbwritertest"), 1024 * 1024 * 1024, 1, 1, 512 * 1024);
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
        lmdbBackend = new LMDBBackend(Paths.get("target/lmdbwritertest"), 1024 * 1024 * 1024, 1, 1, 512 * 1024);
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

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void thatWritingAnExstingPositionIsIllegal() {
        writeDummy("my-position");
        writeDummy("my-position");
    }

    @Test
    public void thatLotsOfReadAndWriteConcurrencyCompletes() throws InterruptedException {
        lmdbBackend.close();
        lmdbBackend = null;
        lmdbBackend = new LMDBBackend(Paths.get("target/lmdb/concurrency-test"), 64 * 1024 * 1024, 20, 80, 64);
        lmdbBackend.drop();
        CountDownLatch latch = new CountDownLatch(100);
        final AtomicInteger nextReaderId = new AtomicInteger(0);
        Runnable readOperation = () -> {
            int readerId = nextReaderId.incrementAndGet();
            try {
                Random random = new Random();
                for (int i = 0; i < 10; i++) {
                    nap(random, 100);
                    String firstPosition = lmdbBackend.getFirstPosition();
                    if (firstPosition != null) {
                        nap(random, 20);
                        lmdbBackend.readContentOf(firstPosition);
                    }
                    nap(random, 100);
                    String lastPosition = lmdbBackend.getLastPosition();
                    if (lastPosition != null) {
                        nap(random, 20);
                        lmdbBackend.readContentOf(lastPosition);
                    }
                    nap(random, 100);
                    String firstPositionForGetSequence = lmdbBackend.getFirstPosition();
                    if (firstPositionForGetSequence != null) {
                        List<String> sequence = lmdbBackend.getSequence(firstPositionForGetSequence, true, 1000);
                        //System.out.printf("Reader %d got sequence: %s%n", readerId, sequence);
                    }
                    nap(random, 100);
                    String firstPositionForReadBulk = lmdbBackend.getFirstPosition();
                    if (firstPositionForReadBulk != null) {
                        lmdbBackend.readContentBulk(firstPositionForReadBulk, true, 1000);
                    }
                }
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                latch.countDown();
            }
        };
        final AtomicInteger nextWriterId = new AtomicInteger(0);
        Runnable writeOperation = () -> {
            int writerId = nextWriterId.incrementAndGet();
            try {
                Random random = new Random();
                writeDummy(writerId + "-start");
                for (int i = 0; i < 10; i++) {
                    nap(random, 10);
                    writeDummy(writerId + "-" + i);
                }
                nap(random, 10);
                writeDummy(writerId + "-end");
            } catch (Throwable t) {
                t.printStackTrace();
            } finally {
                latch.countDown();
            }
        };
        for (int i = 0; i < 80; i++) {
            new Thread(readOperation).start();
        }
        for (int i = 0; i < 20; i++) {
            new Thread(writeOperation).start();
        }
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        List<String> sequence = lmdbBackend.getSequence(lmdbBackend.getFirstPosition(), true, 100000);
        assertEquals(sequence.size(), 20 * 12);
    }

    private void writeDummy(String position) {
        lmdbBackend.write(new LMDBRawdataMessage(position, Map.of(
                "three", new byte[3],
                "four", new byte[4],
                "five", new byte[5],
                "six", new byte[6],
                "seven", new byte[7]
        )));
    }

    private void nap(Random random, int maxMillis) {
        try {
            Thread.sleep(random.nextInt(maxMillis));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
