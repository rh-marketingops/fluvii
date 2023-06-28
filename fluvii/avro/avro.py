from confluent_kafka.schema_registry.avro import AvroSerializer, schemaless_writer, AvroDeserializer, pack, unpack, _ContextStringIO, _MAGIC_BYTE, _schema_loads
from json import loads
from fastavro import (parse_schema,
                      schemaless_reader,
                      schemaless_writer)
from confluent_kafka.serialization import SerializationError

class FluviiAvroSerializer(AvroSerializer) :

    def __call__(self, obj, ctx):
        """
        Serializes an object to the Confluent Schema Registry's Avro binary
        format.

        Args:
            obj (object): object instance to serializes.

            ctx (SerializationContext): Metadata pertaining to the serialization operation.

        Note:
            None objects are represented as Kafka Null.

        Raises:
            SerializerError: if any error occurs serializing obj

        Returns:
            bytes: Confluent Schema Registry formatted Avro bytes

        """
        if obj is None:
            return None

        subject = self._subject_name_func(ctx, self._schema_name)
        if subject not in self._known_subjects:
            if self._use_latest_version:
                latest_schema = self._registry.get_latest_version(subject)
                self._schema_id = latest_schema.schema_id
               
            else:
                # Check to ensure this schema has been registered under subject_name.
                if self._auto_register:
                    # The schema name will always be the same. We can't however register
                    # a schema without a subject so we set the schema_id here to handle
                    # the initial registration.
                    self._schema_id = self._registry.register_schema(subject,
                                                                     self._schema)
                    
                else:
                    registered_schema = self._registry.lookup_schema(subject,
                                                                     self._schema)
                    self._schema_id = registered_schema.schema_id
                    #print(self._schema_id)
            self._known_subjects.add(subject)
       
        if self._to_dict is not None:
            value = self._to_dict(obj, ctx)
        else:
            value = obj
        with _ContextStringIO() as fo: 
            max_int64 = 0xFFFFFFFFFFFFFFFF          
            # Write the magic byte and schema ID in network byte order (big endian)
            #fo.write(pack('>bI', _MAGIC_BYTE, self._schema_id))
            fo.write(pack('>bQQ', _MAGIC_BYTE, (self._schema_id >> 64) & max_int64, self._schema_id & max_int64))
            # write the record to the rest of the buffer
            schemaless_writer(fo, self._parsed_schema, value)

            return fo.getvalue()

class FluviiAvroDeSerializer(AvroDeserializer):
    def __call__(self, value, ctx):
        """
        Decodes a Confluent Schema Registry formatted Avro bytes to an object.

        Arguments:
            value (bytes): bytes

            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.

        Raises:
            SerializerError: if an error occurs ready data.

        Returns:
            object: object if ``from_dict`` is set, otherwise dict. If no value is supplied None is returned.

        """  # noqa: E501
        if value is None:
            return None

        if len(value) <= 5:
            raise SerializationError("Message too small. This message was not"
                                     " produced with a Confluent"
                                     " Schema Registry serializer")

        with _ContextStringIO(value) as payload:
           
            magic, schema_id1, schema_id2 = unpack('>bQQ', payload.read(17))
            schema_id = (schema_id1 << 64) | schema_id2
            if magic != _MAGIC_BYTE:
                raise SerializationError("Unknown magic byte. This message was"
                                         " not produced with a Confluent"
                                         " Schema Registry serializer")

            writer_schema = self._writer_schemas.get(schema_id, None)

            if writer_schema is None:
                schema = self._registry.get_schema(schema_id)
                prepared_schema = _schema_loads(schema.schema_str)
                writer_schema = parse_schema(loads(
                    prepared_schema.schema_str))
                self._writer_schemas[schema_id] = writer_schema

            obj_dict = schemaless_reader(payload,
                                         writer_schema,
                                         self._reader_schema,
                                         self._return_record_name)

            if self._from_dict is not None:
                return self._from_dict(obj_dict, ctx)

            return obj_dict