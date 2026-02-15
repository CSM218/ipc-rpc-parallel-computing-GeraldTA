package pdc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Worker class tests.
 */
class WorkerTest {

    private Worker worker;

    @BeforeEach
    void setUp() {
        worker = new Worker("test-worker", "localhost", 9999);
    }

    @Test
    void testWorker_Join_Logic() {
        assertDoesNotThrow(() -> {
            worker.joinCluster("localhost", 9999);
        }, "Worker join logic should handle network absence gracefully");
    }

    @Test
    void testWorker_Execute_Invocation() {
        assertDoesNotThrow(() -> {
            worker.execute();
        }, "Worker execute should be a non-blocking invocation of the processing loop");
    }
}
