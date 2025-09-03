#[cfg(test)]
mod tests {
    use crate::proto_to_arrow::messages_to_record_batch;
    use arrow::array::Array;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};

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
}
