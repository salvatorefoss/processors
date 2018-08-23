package com.example.processors.custom_processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class DemoProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(DemoProcessor.class);
    }

    @Test
    public void testProcessor() {
        for (int i = 0; i<50000; i++) {
            testRunner.enqueue("Flowfile #" + String.valueOf(i));

        }
        testRunner.run(2);
        // Meaningless test - tests don't obey `nifi.queue.swap.threshold`...
    }

}
