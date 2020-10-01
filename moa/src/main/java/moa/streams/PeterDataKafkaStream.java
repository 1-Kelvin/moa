package moa.streams;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstancesHeader;
import moa.capabilities.CapabilitiesHandler;
import moa.capabilities.Capability;
import moa.capabilities.ImmutableCapabilities;
import moa.core.Example;
import moa.core.ObjectRepository;
import moa.options.AbstractOptionHandler;
import moa.streams.clustering.ClusterEvent;
import moa.streams.generators.cd.ConceptDriftGenerator;
import moa.tasks.TaskMonitor;

import java.util.ArrayList;

public class PeterDataKafkaStream extends AbstractOptionHandler implements
        InstanceStream, ConceptDriftGenerator, CapabilitiesHandler {

    private static final long serialVersionUID = 99L;

    @Override
    protected void prepareForUseImpl(TaskMonitor monitor, ObjectRepository repository) {

    }

    @Override
    public ArrayList<ClusterEvent> getEventsList() {
        return null;
    }

    @Override
    public InstancesHeader getHeader() {
        return null;
    }

    @Override
    public long estimatedRemainingInstances() {
        return 0;
    }

    @Override
    public boolean hasMoreInstances() {
        return false;
    }

    @Override
    public Example<Instance> nextInstance() {
        return null;
    }

    @Override
    public boolean isRestartable() {
        return false;
    }

    @Override
    public void restart() {

    }

    @Override
    public void getDescription(StringBuilder sb, int indent) {

    }

    @Override
    public ImmutableCapabilities defineImmutableCapabilities() {
        if (this.getClass() == PeterDataKafkaStream.class)
            return new ImmutableCapabilities(Capability.VIEW_STANDARD, Capability.VIEW_LITE);
        else
            return new ImmutableCapabilities(Capability.VIEW_STANDARD);
    }
}
