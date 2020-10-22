package moa.streams.kafka.avroConverter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import com.yahoo.labs.samoa.instances.Instance;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroConverterImplementation extends AvroConverter {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(AvroConverterImplementation.class);

    /** The Character reader for JSON read */
    protected Reader reader = null;

    public AvroConverterImplementation(Schema schema) {
        super(schema);
    }

    @Override
    public void initializeSchema(Schema schema) {

        this.schema = schema;
        this.datumReader = new GenericDatumReader<GenericRecord>(schema);
        this.instanceInformation = getHeader();
        this.isSparseData = isSparseData();
    }

    @Override
    public Instance readInstance(GenericRecord record) {
        if (isSparseData)
            return readInstanceSparse(record);
        return readInstanceDense(record);
    }
}
