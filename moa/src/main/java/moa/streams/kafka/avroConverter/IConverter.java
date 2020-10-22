package moa.streams.kafka.avroConverter;

import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstanceInformation;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;

public interface IConverter extends Serializable {
    /**
     * Fetch the Meta-data from the data
     *
     * @return InstanceInformation
     */
    public InstanceInformation getStructure();

    /**
     * Read a single instance from the Stream
     *
     * @return Instance
     */
    public Instance readInstance(GenericRecord record);
}
