package moa.streams.kafka.avroConverter;

import avro.shaded.com.google.common.collect.Lists;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstanceInformation;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import java.util.*;

public class AvroStreamConverter implements IConverter {

    private HashMap<String, List<String>> metaInfos = new HashMap<String, List<String>>() {
        {
            put("forMeter.serialNumber.id", new ArrayList<String>() {
                {
                    add("eba5b585-8bd3-498a-a3bc-6f8cd4b9f975");
                    add("4eb24280-15f9-4a3e-92a5-5cf3a355802d");
                    add("7300f552-9a41-4daf-96b5-4dc6851b7828");
                    add("1b4399e4-3484-4930-8f36-bdc17d2b26f7");
                }
            });
        }
    };

    private Schema _schema;
    private InstanceInformation _instanceInformation;
    private List<Attribute> _attributes;

    public AvroStreamConverter(Schema schema) {
        _schema = schema;
    }

    @Override
    public InstanceInformation getStructure() {
        if(_instanceInformation != null){
            return _instanceInformation;
        }
        List<Attribute> attributes = new ArrayList<>();
        updateAttributeListFromSchema(attributes, _schema, "");
        _attributes = attributes;
        _instanceInformation = new InstanceInformation("avro stream", attributes);
        return _instanceInformation;
    }

    private void updateAttributeListFromSchema(List<Attribute> attributes, Schema schema, String name) {
        switch (schema.getType()) {
            case RECORD:
                for (Schema.Field field : schema.getFields()) {
                    String newName = name.length() <= 0 ? field.name() : name + "." + field.name();
                    updateAttributeListFromSchema(attributes, field.schema(), newName);
                }
                break;
            case STRING:
                if (metaInfos.containsKey(name)) {
                    attributes.add(new Attribute(name, metaInfos.get(name)));
                } else {
                    String[] splitString = name.split("\\.");
                    String lastName = splitString.length <= 0 ? name : splitString[splitString.length - 1];
                    if (lastName.toLowerCase().contains("time"))
                        attributes.add(new Attribute(name));
                }
                break;
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                attributes.add(new Attribute(name));
                break;
            case ENUM:
            case ARRAY:
            case MAP:
            case UNION:
            case FIXED:
            case NULL:
            case BYTES:
                break;
        }
    }


    @Override
    public Instance readInstance(GenericRecord record) {
        getStructure();
        Instance instance = new DenseInstance(_instanceInformation.numAttributes());
        int attributeCount = 0;
        for (Attribute atr : _attributes) {

            String nameSplit[] = atr.name().split("\\.");

            GenericRecord deeperRecord = record;
            for (int i = 0; i < nameSplit.length - 1; i++) {
                deeperRecord = (GenericRecord) deeperRecord.get(nameSplit[i]);
            }
            Schema schema = deeperRecord.getSchema();
            switch (schema.getField(nameSplit[nameSplit.length - 1]).schema().getType()) {
                case STRING:
                    if (nameSplit[nameSplit.length - 1].toLowerCase().contains("time")) {
                        setValue(instance, attributeCount, 0, true);
                    } else {
                        String value = deeperRecord.get(nameSplit[nameSplit.length - 1]).toString();
                        setValue(instance, attributeCount, _instanceInformation.attribute(attributeCount).indexOfValue(value), false);
                        System.out.println(value);
                    }
                    break;
                case INT:
                    break;
                case LONG:
                    break;
                case FLOAT:
                    break;
                case DOUBLE:
                    double dValue = (double) deeperRecord.get(nameSplit[nameSplit.length - 1]);
                    setValue(instance, attributeCount, dValue, true);
                    System.out.println(dValue);
                    break;
                case BOOLEAN:
                    break;
            }
            attributeCount++;
        }

        int currentAttributeNumber = 0;
        return instance;
    }

    protected void setValue(Instance instance, int numAttribute, double value, boolean isNumber) {
        double valueAttribute;

        if (isNumber && this._instanceInformation.attribute(numAttribute).isNominal()) {
            valueAttribute = this._instanceInformation.attribute(numAttribute).indexOfValue(Double.toString(value));
            //System.out.println(value +"/"+valueAttribute+" ");

        } else {
            valueAttribute = value;
            //System.out.println(value +"/"+valueAttribute+" ");
        }
        if (this._instanceInformation.classIndex() == numAttribute) {
            instance.setValue(_instanceInformation.classIndex(), valueAttribute);
            //System.out.println(value +"<"+this.instanceInformation.classIndex()+">");
        } else {
            //if(numAttribute>this.instanceInformation.classIndex())
            //	numAttribute--;
            instance.setValue(numAttribute, valueAttribute);
        }
    }


}
