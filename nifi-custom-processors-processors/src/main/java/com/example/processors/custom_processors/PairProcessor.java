package com.example.processors.custom_processors;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"Pair Files"})
@CapabilityDescription("Pairs Files based on a key")
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class PairProcessor extends AbstractSessionFactoryProcessor {
    public static final Relationship A = new Relationship.Builder()
            .name("A")
            .description("A")
            .build();

    public static final Relationship B = new Relationship.Builder()
            .name("B")
            .description("B")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(A);
        relationships.add(B);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    private final AtomicReference<ProcessSession> hold_session = new AtomicReference<>();

    private final Map<Long, FlowFile> heldFlowfiles = new HashMap<>();
    private final Random random = new Random();

    private Long determineKey(FlowFile flowfile) {
        // Add an implementation to retrieve a key here
        return random.nextLong() % 1000000;
    }

    @OnStopped
    public void whenStopped() {
        // The rollback pushes the flowfiles in the held session back into the connection it came from.
        hold_session.get().rollback();
        hold_session.get().adjustCounter("PairProcessor.Map.Size", -heldFlowfiles.size(), true);
        heldFlowfiles.clear();
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSessionFactory processSessionFactory) throws ProcessException {
        hold_session.compareAndSet(null, processSessionFactory.createSession());
        ProcessSession session = processSessionFactory.createSession();
        List<FlowFile> flowfiles = session.get(10000);

        if (flowfiles.size() == 0) {
            return;
        }

        FlowFile pairedFlowfile;
        for (FlowFile flowfile: flowfiles) {
            Long key = determineKey(flowfile);
            if (heldFlowfiles.containsKey(key)) {
                session.adjustCounter("PairProcessor.Paired", 1, true);
                session.adjustCounter("PairProcessor.Map.Size", -1, true);
                pairedFlowfile = heldFlowfiles.remove(key);
                hold_session.get().migrate(session, Collections.singleton(pairedFlowfile));
                session.transfer(session.putAllAttributes(pairedFlowfile, flowfile.getAttributes()), A);
                session.transfer(flowfile, B);
            } else {
                session.adjustCounter("PairProcessor.Map.Size", 1, true);
                session.migrate(hold_session.get(), Collections.singleton(flowfile));
                heldFlowfiles.put(key, flowfile);
            }
        }
        session.commit();
    }
}
