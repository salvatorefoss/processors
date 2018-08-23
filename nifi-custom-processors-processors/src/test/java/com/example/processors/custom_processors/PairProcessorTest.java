package com.example.processors.custom_processors;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class PairProcessorTest {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(PairProcessor.class);
    }

    @Test
    public void testProcessor() {
        for (int i = 0; i<50000; i++) {
            testRunner.enqueue("Flowfile #" + String.valueOf(i));

        }
        testRunner.run(20);
        // Meaningless - sort of hard to test this when tests don't obey settings - 'Push to Prod!' and #testinprod.
    }

}
