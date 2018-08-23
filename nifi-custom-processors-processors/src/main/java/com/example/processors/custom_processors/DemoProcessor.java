package com.example.processors.custom_processors;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tags({"example"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class DemoProcessor extends AbstractProcessor {
    public static final Relationship MY_RELATIONSHIP = new Relationship.Builder()
            .name("MY_RELATIONSHIP")
            .description("Example relationship")
            .build();


    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(MY_RELATIONSHIP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    public class CustomFlowFileFilter implements FlowFileFilter {
        private int count = 0;
        @Override
        public FlowFileFilterResult filter(FlowFile flowFile) {
            count += 1;
            return FlowFileFilterResult.ACCEPT_AND_CONTINUE;
        }
        public int getCount() {
            return count;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        CustomFlowFileFilter customFlowFileFilter = new CustomFlowFileFilter();

        List<FlowFile> flowfiles = session.get(customFlowFileFilter);
        if ( flowfiles.size() == 0 ) {
            return;
        }

        getLogger().warn("Retrieved " + customFlowFileFilter.getCount() + " flowfiles this execution"); // 19999 (when 20000 is set as nifi.queue.swap.threshold
        // (yes flowfiles.size() also works)
        for (FlowFile flowfile: flowfiles) {
            session.transfer(flowfile, MY_RELATIONSHIP);
        }
    }
}
