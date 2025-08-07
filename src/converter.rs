#[cfg(test)]
mod tests {
    use crate::proto_to_arrow::convert_timestamps;
    use crate::proto_to_arrow::messages_to_record_batch;
    use arrow::array::{Array, BooleanArray, TimestampNanosecondArray};
    use arrow_schema::{DataType, Field};
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
    use std::sync::Arc;

    fn file_descriptor_proto_fixture() -> FileDescriptorProto {
        FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("TestMessage".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("id".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("name".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::String.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn dynamic_messages_fixture(message_descriptor: &MessageDescriptor) -> Vec<DynamicMessage> {
        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("id", prost_reflect::Value::I32(1));
        message1.set_field_by_name("name", prost_reflect::Value::String("test".to_string()));

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("id", prost_reflect::Value::I32(2));
        message2.set_field_by_name("name", prost_reflect::Value::String("test2".to_string()));

        vec![message1, message2]
    }

    #[test]
    fn test_file_descriptor_to_message_descriptor() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor_proto)
            .unwrap();
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        assert_eq!(message_descriptor.name(), "TestMessage");
        assert_eq!(message_descriptor.fields().len(), 2);
        let id_field = message_descriptor.get_field_by_name("id").unwrap();
        assert_eq!(id_field.kind(), prost_reflect::Kind::Int32);
        let name_field = message_descriptor.get_field_by_name("name").unwrap();
        assert_eq!(name_field.kind(), prost_reflect::Kind::String);
    }

    #[test]
    fn test_message_descriptor_fields() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor_proto)
            .unwrap();
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        let id_field = message_descriptor.get_field_by_name("id").unwrap();
        assert_eq!(id_field.number(), 1);
        assert_eq!(id_field.cardinality(), prost_reflect::Cardinality::Optional);

        let name_field = message_descriptor.get_field_by_name("name").unwrap();
        assert_eq!(name_field.number(), 2);
        assert_eq!(
            name_field.cardinality(),
            prost_reflect::Cardinality::Optional
        );
    }

    #[test]
    fn test_dynamic_messages_to_record_batch() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor_proto)
            .unwrap();
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();
        let messages = dynamic_messages_fixture(&message_descriptor);

        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(record_batch.num_columns(), 2);

        let id_array = record_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(id_array, &arrow::array::Int32Array::from(vec![1, 2]));

        let name_array = record_batch
            .column_by_name("name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(
            name_array,
            &arrow::array::StringArray::from(vec!["test", "test2"])
        );
    }

    #[test]
    fn test_convert_timestamps() {
        let seconds_field = Arc::new(Field::new("seconds", DataType::Int64, true));
        let nanos_field = Arc::new(Field::new("nanos", DataType::Int32, true));

        //let seconds = vec![1710330693i64, 1710330702i64];
        let seconds_array: Arc<dyn Array> = Arc::new(arrow::array::Int64Array::from(vec![
            1710330693i64,
            1710330702i64,
            0i64,
        ]));
        let nanos_array: Arc<dyn Array> =
            Arc::new(arrow::array::Int32Array::from(vec![1_000, 123_456_789, 0]));

        let arrays = vec![(seconds_field, seconds_array), (nanos_field, nanos_array)];

        let valid = vec![true, true, false];
        let results = convert_timestamps(&arrays, &valid);
        assert_eq!(results.len(), 3);

        let expected: TimestampNanosecondArray = arrow::array::Int64Array::from(vec![
            1710330693i64 * 1_000_000_000i64 + 1_000i64,
            1710330702i64 * 1_000_000_000i64 + 123_456_789i64,
            0,
        ])
        .reinterpret_cast();

        let mask = BooleanArray::from(vec![false, false, true]);
        let expected_with_null = arrow::compute::nullif(&expected, &mask).unwrap();

        assert_eq!(
            results.as_ref().to_data(),
            expected_with_null.as_ref().to_data()
        )
    }

    #[test]
    fn test_convert_timestamps_empty() {
        let seconds_field = Arc::new(Field::new("seconds", DataType::Int64, true));
        let nanos_field = Arc::new(Field::new("nanos", DataType::Int32, true));

        let seconds_array: Arc<dyn Array> =
            Arc::new(arrow::array::Int64Array::from(Vec::<i64>::new()));
        let nanos_array: Arc<dyn Array> =
            Arc::new(arrow::array::Int32Array::from(Vec::<i32>::new()));

        let arrays = vec![(seconds_field, seconds_array), (nanos_field, nanos_array)];
        let valid: Vec<bool> = vec![];
        let results = convert_timestamps(&arrays, &valid);
        assert_eq!(results.len(), 0);

        let expected: TimestampNanosecondArray =
            arrow::array::Int64Array::from(Vec::<i64>::new()).reinterpret_cast();
        assert_eq!(results.as_ref(), &expected)
    }
}
