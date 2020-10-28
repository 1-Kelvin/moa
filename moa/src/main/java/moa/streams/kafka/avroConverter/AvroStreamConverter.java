package moa.streams.kafka.avroConverter;

import avro.shaded.com.google.common.base.Function;
import avro.shaded.com.google.common.collect.Iterables;
import avro.shaded.com.google.common.collect.Lists;
import com.yahoo.labs.samoa.instances.Instance;
import com.yahoo.labs.samoa.instances.InstanceInformation;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AvroStreamConverter implements IConverter {
	private Schema _schema;

	public AvroStreamConverter(Schema schema) {
		_schema = schema;
		enrichSchemaWithCustomEnum();
	}

	private void enrichSchemaWithCustomEnum() {
//        Schema serialNumberSchema = _schema.getField("forMeter").schema()
//                .getField("serialNumber").schema();
//
//        Schema.Field idField = serialNumberSchema.getField("id");
//
//        Schema newSerialNumberSchema = Schema.createEnum(idField.name(), idField.doc(), "", Arrays.asList(
//           "Eeba5b5858bd3498aa3bc6f8cd4b9f975",
//           "E4eb2428015f94a3e92a55cf3a355802d",
//           "E7300f5529a414daf96b54dc6851b7828",
//           "E1b4399e4348449308f36bdc17d2b26f7"
//        ));
//
//        Schema.Field[] fieldsOfSerialNumber = new Schema.Field[serialNumberSchema.getFields().size()];
//        fieldsOfSerialNumber = serialNumberSchema.getFields().toArray(fieldsOfSerialNumber);
//        fieldsOfSerialNumber[0] = new Schema.Field(idField.name(), newSerialNumberSchema, idField.doc(), idField.defaultVal());

		//serialNumberSchema.(Arrays.asList(fieldsOfSerialNumber));
//        System.out.println(idField.toString());
		Schema newSchema = Schema.createRecord("enrichedSchema", _schema.getDoc(), _schema.getNamespace(), _schema.isError());

		List<Schema.Field> fields = _schema.getFields();
		Iterable<Schema.Field> fieldsNew = Iterables.transform(fields, new Function<Schema.Field, Schema.Field>() {
			@Override
			public Schema.Field apply(Schema.Field input) {
				//this should never happen but the API has marked input as Nullable
				if (null == input) {
					return null;
				}
//                Schema.Field field = new Schema.Field(input.name(), input.schema(), input.doc(), input.defaultValue(), input.order());
				if (input.schema().getField("serialNumber") != null) {
//					System.out.println(input.schema().getField("serialNumber").schema());
					Schema.Field serialNumber = input.schema().getField("serialNumber");
					Schema idSchema = Schema.createEnum(serialNumber.name(), serialNumber.doc(), "", Arrays.asList(
							"Eeba5b5858bd3498aa3bc6f8cd4b9f975",
							"E4eb2428015f94a3e92a55cf3a355802d",
							"E7300f5529a414daf96b54dc6851b7828",
							"E1b4399e4348449308f36bdc17d2b26f7"
					));

					Schema.Field field = new Schema.Field(input.name(), idSchema, input.doc(), input.order());

					input.schema().getField("serialNumber").schema().setFields(Arrays.asList(field));
				}

				System.out.println(input);
				return input;
			}
		});

		newSchema.setFields(Lists.newArrayList(fieldsNew));
	}


	@Override
	public InstanceInformation getStructure() {
		return null;
	}

	@Override
	public Instance readInstance(GenericRecord record) {
		return null;
	}


}
