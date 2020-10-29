package moa.streams.kafka.avroConverter;

import com.yahoo.labs.samoa.instances.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class AvroStreamConverter {

    private final HashMap<String, List<String>> MetaInfoEnumList = new HashMap<String, List<String>>();

    private final HashMap<String, String> MetaInfoDateList = new HashMap<String, String>();

    private Schema _schema;
    private InstanceInformation _instanceInformation;
    private Instances _instances;
    private List<Attribute> _attributes;
    private InstancesHeader _instancesHeader;

    public AvroStreamConverter(Schema schema) {
        _schema = schema;
        extractStructure();
    }

    public InstanceInformation getInstanceInformation(){
        return _instanceInformation;
    }

    public List<Attribute> getAttributes(){
        return _attributes;
    }

    public Instances getInstances(){
        return _instances;
    }

    public InstancesHeader getInstancesHeader(){
        return _instancesHeader;
    }

    public void AddMetaInfoEnumList(String schemaIdentifier, List<String> listOfPossibleValues){
        MetaInfoEnumList.put(schemaIdentifier, listOfPossibleValues);
        extractStructure();
    }

    public void AddMetaInfoDate(String schemaIdentifier, String dateTimeFormat){
        MetaInfoDateList.put(schemaIdentifier, dateTimeFormat);
        extractStructure();
    }

    private void extractStructure(){
        List<Attribute> attributes = new ArrayList<>();
        updateAttributeListFromSchema(attributes, _schema, "");
        _attributes = attributes;
        _instanceInformation = new InstanceInformation("avro stream", attributes);
        _instances = new Instances("avro stream", _attributes, 0);
        _instancesHeader = new InstancesHeader(_instances);
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
                if (MetaInfoEnumList.containsKey(name)) {
                    attributes.add(new Attribute(name, MetaInfoEnumList.get(name)));
                } else if(MetaInfoDateList.containsKey(name)) {
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
                System.out.println("Cant handle type: " + schema.getType() + " of Avro schema");
                break;
        }
    }

    public Instance readInstance(GenericRecord record) {
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
                    if(MetaInfoDateList.containsKey(atr.name())){
                        try{
                            String dateString = deeperRecord.get(nameSplit[nameSplit.length - 1]).toString();
                            DateFormat df = new SimpleDateFormat(MetaInfoDateList.get(atr.name()));
                            Date date = df.parse(dateString);
                            setValue(instance, attributeCount, date.getTime(), true);
                        }catch (ParseException ex){
                            setValue(instance, attributeCount, -1, true);
                        }
                    } else if (MetaInfoEnumList.containsKey(atr.name())) {
                        String value = deeperRecord.get(nameSplit[nameSplit.length - 1]).toString();
                        setValue(instance, attributeCount, _instanceInformation.attribute(attributeCount).indexOfValue(value), false);
                    }
                    break;
                case INT:
                    int iValue = (int) deeperRecord.get(nameSplit[nameSplit.length - 1]);
                    setValue(instance, attributeCount, iValue, true);
                    break;
                case LONG:
                case FLOAT:
                case DOUBLE:
                    double dValue = (double) deeperRecord.get(nameSplit[nameSplit.length - 1]);
                    setValue(instance, attributeCount, dValue, true);
                    break;
                case BOOLEAN:
                    if((boolean)deeperRecord.get(nameSplit[nameSplit.length - 1])){
                        setValue(instance, attributeCount, 1, true);
                    }else{
                        setValue(instance, attributeCount, 0, true);
                    }
                    break;
            }
            attributeCount++;
        }
        instance.setDataset(_instancesHeader);
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
