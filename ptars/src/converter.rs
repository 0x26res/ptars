#[cfg(test)]
mod tests {
    use crate::arrow_to_proto::record_batch_to_array;
    use crate::proto_to_arrow::{
        field_to_array, get_array_builder, get_singular_array_builder, is_nullable,
        messages_to_record_batch, CE_OFFSET,
    };
    use arrow::array::Array;
    use chrono::Datelike;
    use prost_reflect::prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto,
    };
    use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};

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

    fn create_pool_with_message(file_descriptor: FileDescriptorProto) -> DescriptorPool {
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(file_descriptor).unwrap();
        pool
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

    // ==================== Primitive Type Tests ====================

    fn create_primitive_message_descriptor(
        field_name: &str,
        field_type: Type,
    ) -> (DescriptorPool, MessageDescriptor) {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("PrimitiveMessage".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some(field_name.to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(field_type.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.PrimitiveMessage").unwrap();
        (pool, message_descriptor)
    }

    #[test]
    fn test_int64_field_conversion() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Int64);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("value", Value::I64(i64::MAX));

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("value", Value::I64(i64::MIN));

        let messages = vec![message1, message2];
        let array = field_to_array(&field, &messages).unwrap();

        let int64_array = array
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(int64_array.value(0), i64::MAX);
        assert_eq!(int64_array.value(1), i64::MIN);
    }

    #[test]
    fn test_uint32_field_conversion() {
        let (_pool, message_descriptor) =
            create_primitive_message_descriptor("value", Type::Uint32);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("value", Value::U32(u32::MAX));

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("value", Value::U32(0));

        let messages = vec![message1, message2];
        let array = field_to_array(&field, &messages).unwrap();

        let uint32_array = array
            .as_any()
            .downcast_ref::<arrow::array::UInt32Array>()
            .unwrap();
        assert_eq!(uint32_array.value(0), u32::MAX);
        assert_eq!(uint32_array.value(1), 0);
    }

    #[test]
    fn test_uint64_field_conversion() {
        let (_pool, message_descriptor) =
            create_primitive_message_descriptor("value", Type::Uint64);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("value", Value::U64(u64::MAX));

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let uint64_array = array
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert_eq!(uint64_array.value(0), u64::MAX);
    }

    #[test]
    fn test_float_field_conversion() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Float);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("value", Value::F32(3.14));

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("value", Value::F32(-2.71));

        let messages = vec![message1, message2];
        let array = field_to_array(&field, &messages).unwrap();

        let float_array = array
            .as_any()
            .downcast_ref::<arrow::array::Float32Array>()
            .unwrap();
        assert!((float_array.value(0) - 3.14).abs() < 0.001);
        assert!((float_array.value(1) - (-2.71)).abs() < 0.001);
    }

    #[test]
    fn test_double_field_conversion() {
        let (_pool, message_descriptor) =
            create_primitive_message_descriptor("value", Type::Double);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("value", Value::F64(std::f64::consts::PI));

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let double_array = array
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((double_array.value(0) - std::f64::consts::PI).abs() < 1e-10);
    }

    #[test]
    fn test_bool_field_conversion() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Bool);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("value", Value::Bool(true));

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("value", Value::Bool(false));

        let messages = vec![message1, message2];
        let array = field_to_array(&field, &messages).unwrap();

        let bool_array = array
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
    }

    #[test]
    fn test_bytes_field_conversion() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Bytes);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name(
            "value",
            Value::Bytes(prost::bytes::Bytes::from(vec![1, 2, 3, 4])),
        );

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("value", Value::Bytes(prost::bytes::Bytes::from(vec![])));

        let messages = vec![message1, message2];
        let array = field_to_array(&field, &messages).unwrap();

        let binary_array = array
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(binary_array.value(0), &[1, 2, 3, 4]);
        assert_eq!(binary_array.value(1), &[] as &[u8]);
    }

    // ==================== Repeated Field Tests ====================

    fn create_repeated_message_descriptor(
        field_name: &str,
        field_type: Type,
    ) -> (DescriptorPool, MessageDescriptor) {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("RepeatedMessage".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some(field_name.to_string()),
                    number: Some(1),
                    label: Some(Label::Repeated.into()),
                    r#type: Some(field_type.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.RepeatedMessage").unwrap();
        (pool, message_descriptor)
    }

    #[test]
    fn test_repeated_int32_field_conversion() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name(
            "values",
            Value::List(vec![Value::I32(1), Value::I32(2), Value::I32(3)]),
        );

        let mut message2 = DynamicMessage::new(message_descriptor.clone());
        message2.set_field_by_name("values", Value::List(vec![Value::I32(4), Value::I32(5)]));

        let messages = vec![message1, message2];
        let array = field_to_array(&field, &messages).unwrap();

        let list_array = array
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(list_array.len(), 2);

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(values.values(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_repeated_string_field_conversion() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::String);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name(
            "values",
            Value::List(vec![
                Value::String("hello".to_string()),
                Value::String("world".to_string()),
            ]),
        );

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let list_array = array
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(list_array.len(), 1);

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "hello");
        assert_eq!(values.value(1), "world");
    }

    #[test]
    fn test_repeated_bool_field_conversion() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Bool);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name(
            "values",
            Value::List(vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
            ]),
        );

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let list_array = array
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();
        assert!(values.value(0));
        assert!(!values.value(1));
        assert!(values.value(2));
    }

    #[test]
    fn test_repeated_bytes_field_conversion() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Bytes);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name(
            "values",
            Value::List(vec![
                Value::Bytes(prost::bytes::Bytes::from(vec![1, 2])),
                Value::Bytes(prost::bytes::Bytes::from(vec![3, 4, 5])),
            ]),
        );

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let list_array = array
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<arrow::array::BinaryArray>()
            .unwrap();
        assert_eq!(values.value(0), &[1, 2]);
        assert_eq!(values.value(1), &[3, 4, 5]);
    }

    #[test]
    fn test_repeated_double_field_conversion() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Double);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name(
            "values",
            Value::List(vec![Value::F64(1.1), Value::F64(2.2), Value::F64(3.3)]),
        );

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let list_array = array
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        let values = list_array
            .values()
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap();
        assert!((values.value(0) - 1.1).abs() < 1e-10);
        assert!((values.value(1) - 2.2).abs() < 1e-10);
        assert!((values.value(2) - 3.3).abs() < 1e-10);
    }

    #[test]
    fn test_empty_repeated_field() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut message1 = DynamicMessage::new(message_descriptor.clone());
        message1.set_field_by_name("values", Value::List(vec![]));

        let messages = vec![message1];
        let array = field_to_array(&field, &messages).unwrap();

        let list_array = array
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.value_length(0), 0);
    }

    // ==================== Nested Message Tests ====================

    #[test]
    fn test_nested_message_conversion() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("InnerMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("inner_id".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("inner_name".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("OuterMessage".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("outer_id".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("inner".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".test.InnerMessage".to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let outer_descriptor = pool.get_message_by_name("test.OuterMessage").unwrap();
        let inner_descriptor = pool.get_message_by_name("test.InnerMessage").unwrap();

        let mut inner1 = DynamicMessage::new(inner_descriptor.clone());
        inner1.set_field_by_name("inner_id", Value::I32(100));
        inner1.set_field_by_name("inner_name", Value::String("inner_one".to_string()));

        let mut outer1 = DynamicMessage::new(outer_descriptor.clone());
        outer1.set_field_by_name("outer_id", Value::I32(1));
        outer1.set_field_by_name("inner", Value::Message(inner1));

        let messages = vec![outer1];
        let record_batch = messages_to_record_batch(&messages, &outer_descriptor);

        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 2);

        let outer_id = record_batch
            .column_by_name("outer_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(outer_id.value(0), 1);

        let inner_struct = record_batch
            .column_by_name("inner")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StructArray>()
            .unwrap();

        let inner_id = inner_struct
            .column_by_name("inner_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(inner_id.value(0), 100);

        let inner_name = inner_struct
            .column_by_name("inner_name")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        assert_eq!(inner_name.value(0), "inner_one");
    }

    // ==================== Builder Tests ====================

    #[test]
    fn test_primitive_builder_wrapper_len_and_is_empty() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Int32);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut builder = get_singular_array_builder(&field).unwrap();
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.append(&Value::I32(42));
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);

        builder.append(&Value::I32(43));
        assert_eq!(builder.len(), 2);
    }

    #[test]
    fn test_primitive_builder_append_null() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Int32);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut builder = get_singular_array_builder(&field).unwrap();
        builder.append(&Value::I32(1));
        builder.append_null();
        builder.append(&Value::I32(3));

        let array = builder.finish();
        assert_eq!(array.len(), 3);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_string_builder_append_null() {
        let (_pool, message_descriptor) =
            create_primitive_message_descriptor("value", Type::String);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut builder = get_singular_array_builder(&field).unwrap();
        builder.append(&Value::String("hello".to_string()));
        builder.append_null();
        builder.append(&Value::String("world".to_string()));

        let array = builder.finish();
        assert_eq!(array.len(), 3);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_bool_builder_append_null() {
        let (_pool, message_descriptor) = create_primitive_message_descriptor("value", Type::Bool);
        let field = message_descriptor.get_field_by_name("value").unwrap();

        let mut builder = get_singular_array_builder(&field).unwrap();
        builder.append(&Value::Bool(true));
        builder.append_null();
        builder.append(&Value::Bool(false));

        let array = builder.finish();
        assert_eq!(array.len(), 3);
        assert!(!array.is_null(0));
        assert!(array.is_null(1));
        assert!(!array.is_null(2));
    }

    #[test]
    fn test_repeated_builder_len_and_is_empty() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);
        let field = message_descriptor.get_field_by_name("values").unwrap();

        let mut builder = get_array_builder(&field).unwrap();
        assert!(builder.is_empty());
        assert_eq!(builder.len(), 0);

        builder.append(&Value::List(vec![Value::I32(1), Value::I32(2)]));
        assert!(!builder.is_empty());
        assert_eq!(builder.len(), 1);

        builder.append(&Value::List(vec![Value::I32(3)]));
        assert_eq!(builder.len(), 2);
    }

    // ==================== is_nullable Tests ====================

    #[test]
    fn test_is_nullable_proto3_singular() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("TestMessage".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("value".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(Type::Int32.into()),
                    proto3_optional: Some(true),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();
        let field = message_descriptor.get_field_by_name("value").unwrap();

        // proto3 optional fields support presence
        assert!(is_nullable(&field));
    }

    // ==================== Empty Message Tests ====================

    #[test]
    fn test_empty_message_list() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        let messages: Vec<DynamicMessage> = vec![];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);

        assert_eq!(record_batch.num_rows(), 0);
        assert_eq!(record_batch.num_columns(), 2);
    }

    #[test]
    fn test_message_with_empty_fields() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("EmptyFieldsMessage".to_string()),
                field: vec![],
                ..Default::default()
            }],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.EmptyFieldsMessage").unwrap();

        let message = DynamicMessage::new(message_descriptor.clone());
        let messages = vec![message];

        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        assert_eq!(record_batch.num_rows(), 1);
        assert_eq!(record_batch.num_columns(), 0);
    }

    // ==================== Map Field Tests ====================

    #[test]
    fn test_map_field_conversion() {
        use std::collections::HashMap;
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("MapEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("MessageWithMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("my_map".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.MapEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.MessageWithMap").unwrap();

        let mut map_value: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map_value.insert(
            prost_reflect::MapKey::String("key1".to_string()),
            Value::I32(100),
        );
        map_value.insert(
            prost_reflect::MapKey::String("key2".to_string()),
            Value::I32(200),
        );

        let mut message = DynamicMessage::new(message_descriptor.clone());
        message.set_field_by_name("my_map", Value::Map(map_value));

        let messages = vec![message];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);

        assert_eq!(record_batch.num_rows(), 1);

        let map_array = record_batch
            .column_by_name("my_map")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        assert_eq!(map_array.len(), 1);
        assert_eq!(map_array.value_length(0), 2);
    }

    // ==================== Round-trip Tests ====================

    #[test]
    fn test_roundtrip_simple_message() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        let mut original = DynamicMessage::new(message_descriptor.clone());
        original.set_field_by_name("id", Value::I32(42));
        original.set_field_by_name("name", Value::String("roundtrip".to_string()));

        let messages = vec![original.clone()];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        assert_eq!(binary_array.len(), 1);

        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        assert_eq!(decoded.get_field_by_name("id").unwrap().as_i32(), Some(42));
        assert_eq!(
            decoded.get_field_by_name("name").unwrap().as_str(),
            Some("roundtrip")
        );
    }

    #[test]
    fn test_roundtrip_with_repeated_fields() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);

        let mut original = DynamicMessage::new(message_descriptor.clone());
        original.set_field_by_name(
            "values",
            Value::List(vec![Value::I32(1), Value::I32(2), Value::I32(3)]),
        );

        let messages = vec![original];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_i32(), Some(1));
        assert_eq!(list[1].as_i32(), Some(2));
        assert_eq!(list[2].as_i32(), Some(3));
    }

    #[test]
    fn test_roundtrip_all_primitive_types() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("AllTypes".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("int32_field".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("int64_field".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int64.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("uint32_field".to_string()),
                        number: Some(3),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Uint32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("uint64_field".to_string()),
                        number: Some(4),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Uint64.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("float_field".to_string()),
                        number: Some(5),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Float.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("double_field".to_string()),
                        number: Some(6),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Double.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("bool_field".to_string()),
                        number: Some(7),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Bool.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("string_field".to_string()),
                        number: Some(8),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::String.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("bytes_field".to_string()),
                        number: Some(9),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Bytes.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.AllTypes").unwrap();

        let mut original = DynamicMessage::new(message_descriptor.clone());
        original.set_field_by_name("int32_field", Value::I32(-42));
        original.set_field_by_name("int64_field", Value::I64(-9999999999i64));
        original.set_field_by_name("uint32_field", Value::U32(42));
        original.set_field_by_name("uint64_field", Value::U64(9999999999u64));
        original.set_field_by_name("float_field", Value::F32(3.14));
        original.set_field_by_name("double_field", Value::F64(2.71828));
        original.set_field_by_name("bool_field", Value::Bool(true));
        original.set_field_by_name("string_field", Value::String("test string".to_string()));
        original.set_field_by_name(
            "bytes_field",
            Value::Bytes(prost::bytes::Bytes::from(vec![1, 2, 3, 4, 5])),
        );

        let messages = vec![original];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        assert_eq!(
            decoded.get_field_by_name("int32_field").unwrap().as_i32(),
            Some(-42)
        );
        assert_eq!(
            decoded.get_field_by_name("int64_field").unwrap().as_i64(),
            Some(-9999999999i64)
        );
        assert_eq!(
            decoded.get_field_by_name("uint32_field").unwrap().as_u32(),
            Some(42)
        );
        assert_eq!(
            decoded.get_field_by_name("uint64_field").unwrap().as_u64(),
            Some(9999999999u64)
        );
        assert!(
            (decoded
                .get_field_by_name("float_field")
                .unwrap()
                .as_f32()
                .unwrap()
                - 3.14)
                .abs()
                < 0.001
        );
        assert!(
            (decoded
                .get_field_by_name("double_field")
                .unwrap()
                .as_f64()
                .unwrap()
                - 2.71828)
                .abs()
                < 1e-5
        );
        assert_eq!(
            decoded.get_field_by_name("bool_field").unwrap().as_bool(),
            Some(true)
        );
        assert_eq!(
            decoded.get_field_by_name("string_field").unwrap().as_str(),
            Some("test string")
        );
        assert_eq!(
            decoded
                .get_field_by_name("bytes_field")
                .unwrap()
                .as_bytes()
                .map(|b| b.to_vec()),
            Some(vec![1, 2, 3, 4, 5])
        );
    }

    // ==================== CE_OFFSET Constant Test ====================

    #[test]
    fn test_ce_offset_value() {
        // CE_OFFSET should be the number of days from 0001-01-01 to 1970-01-01
        // This is a known value used for date conversions
        assert_eq!(CE_OFFSET, 719163);

        // Verify it's correct by computing from chrono
        let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        assert_eq!(epoch.num_days_from_ce(), CE_OFFSET);
    }

    // ==================== Multiple Messages Tests ====================

    #[test]
    fn test_multiple_messages_with_varying_values() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        let mut messages = Vec::new();
        for i in 0..100 {
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field_by_name("id", Value::I32(i));
            msg.set_field_by_name("name", Value::String(format!("name_{}", i)));
            messages.push(msg);
        }

        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        assert_eq!(record_batch.num_rows(), 100);

        let id_array = record_batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        for i in 0..100 {
            assert_eq!(id_array.value(i), i as i32);
        }
    }

    // ==================== Repeated Messages Tests ====================

    #[test]
    fn test_repeated_nested_messages() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Item".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("value".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Container".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("items".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.Item".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let container_descriptor = pool.get_message_by_name("test.Container").unwrap();
        let item_descriptor = pool.get_message_by_name("test.Item").unwrap();

        let mut item1 = DynamicMessage::new(item_descriptor.clone());
        item1.set_field_by_name("value", Value::I32(10));

        let mut item2 = DynamicMessage::new(item_descriptor.clone());
        item2.set_field_by_name("value", Value::I32(20));

        let mut container = DynamicMessage::new(container_descriptor.clone());
        container.set_field_by_name(
            "items",
            Value::List(vec![Value::Message(item1), Value::Message(item2)]),
        );

        let messages = vec![container];
        let record_batch = messages_to_record_batch(&messages, &container_descriptor);

        assert_eq!(record_batch.num_rows(), 1);

        let items_array = record_batch
            .column_by_name("items")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();

        assert_eq!(items_array.len(), 1);
        assert_eq!(items_array.value_length(0), 2);
    }

    #[test]
    fn test_repeated_nested_messages_round_trip() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Item".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("value".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Container".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("items".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.Item".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let container_descriptor = pool.get_message_by_name("test.Container").unwrap();
        let item_descriptor = pool.get_message_by_name("test.Item").unwrap();

        let mut item1 = DynamicMessage::new(item_descriptor.clone());
        item1.set_field_by_name("value", Value::I32(10));

        let mut item2 = DynamicMessage::new(item_descriptor.clone());
        item2.set_field_by_name("value", Value::I32(20));

        let mut container = DynamicMessage::new(container_descriptor.clone());
        container.set_field_by_name(
            "items",
            Value::List(vec![Value::Message(item1), Value::Message(item2)]),
        );

        let messages = vec![container];

        // Convert to Arrow
        let record_batch = messages_to_record_batch(&messages, &container_descriptor);

        // Convert back to Proto
        let array_data = record_batch_to_array(&record_batch, &container_descriptor);
        let binary_array = arrow::array::BinaryArray::from(array_data);

        assert_eq!(binary_array.len(), 1);

        // Decode and verify
        let decoded =
            DynamicMessage::decode(container_descriptor.clone(), binary_array.value(0)).unwrap();

        let items = decoded.get_field_by_name("items").unwrap();
        let items_list = items.as_list().unwrap();

        assert_eq!(items_list.len(), 2);
        assert_eq!(
            items_list[0]
                .as_message()
                .unwrap()
                .get_field_by_name("value")
                .unwrap()
                .as_i32(),
            Some(10)
        );
        assert_eq!(
            items_list[1]
                .as_message()
                .unwrap()
                .get_field_by_name("value")
                .unwrap()
                .as_i32(),
            Some(20)
        );
    }

    // ==================== Arrow to Proto Tests ====================

    #[test]
    fn test_arrow_to_proto_multiple_rows() {
        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        // Create multiple messages
        let mut messages = Vec::new();
        for i in 0..5 {
            let mut msg = DynamicMessage::new(message_descriptor.clone());
            msg.set_field_by_name("id", Value::I32(i * 10));
            msg.set_field_by_name("name", Value::String(format!("row_{}", i)));
            messages.push(msg);
        }

        // Convert to Arrow and back
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        assert_eq!(binary_array.len(), 5);

        // Verify each row
        for i in 0..5 {
            let decoded =
                DynamicMessage::decode(message_descriptor.clone(), binary_array.value(i)).unwrap();
            assert_eq!(
                decoded.get_field_by_name("id").unwrap().as_i32(),
                Some(i as i32 * 10)
            );
            assert_eq!(
                decoded.get_field_by_name("name").unwrap().as_str(),
                Some(format!("row_{}", i).as_str())
            );
        }
    }

    #[test]
    fn test_arrow_to_proto_repeated_string() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::String);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::String("alpha".to_string()),
                Value::String("beta".to_string()),
                Value::String("gamma".to_string()),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_str(), Some("alpha"));
        assert_eq!(list[1].as_str(), Some("beta"));
        assert_eq!(list[2].as_str(), Some("gamma"));
    }

    #[test]
    fn test_arrow_to_proto_repeated_bool() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Bool);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::Bool(true),
                Value::Bool(false),
                Value::Bool(true),
                Value::Bool(false),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 4);
        assert_eq!(list[0].as_bool(), Some(true));
        assert_eq!(list[1].as_bool(), Some(false));
        assert_eq!(list[2].as_bool(), Some(true));
        assert_eq!(list[3].as_bool(), Some(false));
    }

    #[test]
    fn test_arrow_to_proto_repeated_bytes() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Bytes);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::Bytes(prost::bytes::Bytes::from(vec![0xDE, 0xAD])),
                Value::Bytes(prost::bytes::Bytes::from(vec![0xBE, 0xEF])),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(
            list[0].as_bytes().map(|b| b.to_vec()),
            Some(vec![0xDE, 0xAD])
        );
        assert_eq!(
            list[1].as_bytes().map(|b| b.to_vec()),
            Some(vec![0xBE, 0xEF])
        );
    }

    #[test]
    fn test_arrow_to_proto_repeated_float() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Float);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![Value::F32(1.5), Value::F32(2.5), Value::F32(3.5)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert!((list[0].as_f32().unwrap() - 1.5).abs() < 0.001);
        assert!((list[1].as_f32().unwrap() - 2.5).abs() < 0.001);
        assert!((list[2].as_f32().unwrap() - 3.5).abs() < 0.001);
    }

    #[test]
    fn test_arrow_to_proto_repeated_int64() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int64);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::I64(i64::MIN),
                Value::I64(0),
                Value::I64(i64::MAX),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_i64(), Some(i64::MIN));
        assert_eq!(list[1].as_i64(), Some(0));
        assert_eq!(list[2].as_i64(), Some(i64::MAX));
    }

    #[test]
    fn test_arrow_to_proto_repeated_uint32() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Uint32);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![Value::U32(0), Value::U32(100), Value::U32(u32::MAX)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_u32(), Some(0));
        assert_eq!(list[1].as_u32(), Some(100));
        assert_eq!(list[2].as_u32(), Some(u32::MAX));
    }

    #[test]
    fn test_arrow_to_proto_repeated_uint64() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Uint64);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![Value::U64(0), Value::U64(u64::MAX)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0].as_u64(), Some(0));
        assert_eq!(list[1].as_u64(), Some(u64::MAX));
    }

    #[test]
    fn test_arrow_to_proto_nested_message_roundtrip() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Inner".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("x".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("y".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Outer".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("name".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("point".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".test.Inner".to_string()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let outer_descriptor = pool.get_message_by_name("test.Outer").unwrap();
        let inner_descriptor = pool.get_message_by_name("test.Inner").unwrap();

        let mut inner = DynamicMessage::new(inner_descriptor.clone());
        inner.set_field_by_name("x", Value::I32(10));
        inner.set_field_by_name("y", Value::I32(20));

        let mut outer = DynamicMessage::new(outer_descriptor.clone());
        outer.set_field_by_name("name", Value::String("point_message".to_string()));
        outer.set_field_by_name("point", Value::Message(inner));

        let messages = vec![outer];
        let record_batch = messages_to_record_batch(&messages, &outer_descriptor);
        let array_data = record_batch_to_array(&record_batch, &outer_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(outer_descriptor.clone(), binary_array.value(0)).unwrap();

        assert_eq!(
            decoded.get_field_by_name("name").unwrap().as_str(),
            Some("point_message")
        );

        let point_value = decoded.get_field_by_name("point").unwrap();
        let point = point_value.as_message().unwrap();
        assert_eq!(point.get_field_by_name("x").unwrap().as_i32(), Some(10));
        assert_eq!(point.get_field_by_name("y").unwrap().as_i32(), Some(20));
    }

    #[test]
    fn test_arrow_to_proto_empty_repeated() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("values", Value::List(vec![]));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 0);
    }

    #[test]
    fn test_arrow_to_proto_single_repeated_element() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("values", Value::List(vec![Value::I32(42)]));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].as_i32(), Some(42));
    }

    #[test]
    fn test_sfixed_and_sint_types() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("FixedTypes".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("sfixed32_field".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Sfixed32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("sfixed64_field".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Sfixed64.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("sint32_field".to_string()),
                        number: Some(3),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Sint32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("sint64_field".to_string()),
                        number: Some(4),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Sint64.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("fixed32_field".to_string()),
                        number: Some(5),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Fixed32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("fixed64_field".to_string()),
                        number: Some(6),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Fixed64.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.FixedTypes").unwrap();

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("sfixed32_field", Value::I32(-123));
        msg.set_field_by_name("sfixed64_field", Value::I64(-456));
        msg.set_field_by_name("sint32_field", Value::I32(-789));
        msg.set_field_by_name("sint64_field", Value::I64(-101112));
        msg.set_field_by_name("fixed32_field", Value::U32(999));
        msg.set_field_by_name("fixed64_field", Value::U64(131415));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        assert_eq!(
            decoded
                .get_field_by_name("sfixed32_field")
                .unwrap()
                .as_i32(),
            Some(-123)
        );
        assert_eq!(
            decoded
                .get_field_by_name("sfixed64_field")
                .unwrap()
                .as_i64(),
            Some(-456)
        );
        assert_eq!(
            decoded.get_field_by_name("sint32_field").unwrap().as_i32(),
            Some(-789)
        );
        assert_eq!(
            decoded.get_field_by_name("sint64_field").unwrap().as_i64(),
            Some(-101112)
        );
        assert_eq!(
            decoded.get_field_by_name("fixed32_field").unwrap().as_u32(),
            Some(999)
        );
        assert_eq!(
            decoded.get_field_by_name("fixed64_field").unwrap().as_u64(),
            Some(131415)
        );
    }

    // ==================== Timestamp Field Tests ====================

    fn create_timestamp_pool() -> DescriptorPool {
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("google/protobuf/timestamp.proto".to_string()),
            package: Some("google.protobuf".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("Timestamp".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("seconds".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int64.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("nanos".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        pool
    }

    fn create_date_pool() -> DescriptorPool {
        let mut pool = DescriptorPool::new();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("google/type/date.proto".to_string()),
            package: Some("google.type".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("Date".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("year".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("month".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("day".to_string()),
                        number: Some(3),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        pool
    }

    #[test]
    fn test_timestamp_field_roundtrip() {
        let mut pool = create_timestamp_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/protobuf/timestamp.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithTimestamp".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("ts".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.protobuf.Timestamp".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithTimestamp").unwrap();
        let timestamp_descriptor = pool
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();

        let mut ts = DynamicMessage::new(timestamp_descriptor.clone());
        ts.set_field_by_name("seconds", Value::I64(1700000000));
        ts.set_field_by_name("nanos", Value::I32(123456789));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("ts", Value::Message(ts));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let ts_value = decoded.get_field_by_name("ts").unwrap();
        let ts_msg = ts_value.as_message().unwrap();
        assert_eq!(
            ts_msg.get_field_by_name("seconds").unwrap().as_i64(),
            Some(1700000000)
        );
        assert_eq!(
            ts_msg.get_field_by_name("nanos").unwrap().as_i32(),
            Some(123456789)
        );
    }

    #[test]
    fn test_timestamp_negative_roundtrip() {
        let mut pool = create_timestamp_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/protobuf/timestamp.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithTimestamp".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("ts".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.protobuf.Timestamp".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithTimestamp").unwrap();
        let timestamp_descriptor = pool
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();

        // Test negative timestamp (before Unix epoch - e.g., 1960)
        let mut ts = DynamicMessage::new(timestamp_descriptor.clone());
        ts.set_field_by_name("seconds", Value::I64(-315619200)); // Jan 1, 1960
        ts.set_field_by_name("nanos", Value::I32(500000000));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("ts", Value::Message(ts));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let ts_value = decoded.get_field_by_name("ts").unwrap();
        let ts_msg = ts_value.as_message().unwrap();
        assert_eq!(
            ts_msg.get_field_by_name("seconds").unwrap().as_i64(),
            Some(-315619200)
        );
        assert_eq!(
            ts_msg.get_field_by_name("nanos").unwrap().as_i32(),
            Some(500000000)
        );
    }

    #[test]
    fn test_repeated_timestamp_roundtrip() {
        let mut pool = create_timestamp_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/protobuf/timestamp.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithTimestamps".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("timestamps".to_string()),
                    number: Some(1),
                    label: Some(Label::Repeated.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.protobuf.Timestamp".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithTimestamps").unwrap();
        let timestamp_descriptor = pool
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();

        let mut ts1 = DynamicMessage::new(timestamp_descriptor.clone());
        ts1.set_field_by_name("seconds", Value::I64(1000));
        ts1.set_field_by_name("nanos", Value::I32(100));

        let mut ts2 = DynamicMessage::new(timestamp_descriptor.clone());
        ts2.set_field_by_name("seconds", Value::I64(2000));
        ts2.set_field_by_name("nanos", Value::I32(200));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "timestamps",
            Value::List(vec![Value::Message(ts1), Value::Message(ts2)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let ts_list = decoded.get_field_by_name("timestamps").unwrap();
        let list = ts_list.as_list().unwrap();
        assert_eq!(list.len(), 2);

        let ts1_decoded = list[0].as_message().unwrap();
        assert_eq!(
            ts1_decoded.get_field_by_name("seconds").unwrap().as_i64(),
            Some(1000)
        );
        assert_eq!(
            ts1_decoded.get_field_by_name("nanos").unwrap().as_i32(),
            Some(100)
        );

        let ts2_decoded = list[1].as_message().unwrap();
        assert_eq!(
            ts2_decoded.get_field_by_name("seconds").unwrap().as_i64(),
            Some(2000)
        );
        assert_eq!(
            ts2_decoded.get_field_by_name("nanos").unwrap().as_i32(),
            Some(200)
        );
    }

    // ==================== Date Field Tests ====================

    #[test]
    fn test_date_field_roundtrip() {
        let mut pool = create_date_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/type/date.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithDate".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("date".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.type.Date".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithDate").unwrap();
        let date_descriptor = pool.get_message_by_name("google.type.Date").unwrap();

        let mut date = DynamicMessage::new(date_descriptor.clone());
        date.set_field_by_name("year", Value::I32(2024));
        date.set_field_by_name("month", Value::I32(12));
        date.set_field_by_name("day", Value::I32(25));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("date", Value::Message(date));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let date_value = decoded.get_field_by_name("date").unwrap();
        let date_msg = date_value.as_message().unwrap();
        assert_eq!(
            date_msg.get_field_by_name("year").unwrap().as_i32(),
            Some(2024)
        );
        assert_eq!(
            date_msg.get_field_by_name("month").unwrap().as_i32(),
            Some(12)
        );
        assert_eq!(
            date_msg.get_field_by_name("day").unwrap().as_i32(),
            Some(25)
        );
    }

    #[test]
    fn test_repeated_date_roundtrip() {
        let mut pool = create_date_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/type/date.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithDates".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("dates".to_string()),
                    number: Some(1),
                    label: Some(Label::Repeated.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.type.Date".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithDates").unwrap();
        let date_descriptor = pool.get_message_by_name("google.type.Date").unwrap();

        let mut date1 = DynamicMessage::new(date_descriptor.clone());
        date1.set_field_by_name("year", Value::I32(2020));
        date1.set_field_by_name("month", Value::I32(1));
        date1.set_field_by_name("day", Value::I32(15));

        let mut date2 = DynamicMessage::new(date_descriptor.clone());
        date2.set_field_by_name("year", Value::I32(2021));
        date2.set_field_by_name("month", Value::I32(6));
        date2.set_field_by_name("day", Value::I32(30));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "dates",
            Value::List(vec![Value::Message(date1), Value::Message(date2)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let dates_list = decoded.get_field_by_name("dates").unwrap();
        let list = dates_list.as_list().unwrap();
        assert_eq!(list.len(), 2);

        let date1_decoded = list[0].as_message().unwrap();
        assert_eq!(
            date1_decoded.get_field_by_name("year").unwrap().as_i32(),
            Some(2020)
        );
        assert_eq!(
            date1_decoded.get_field_by_name("month").unwrap().as_i32(),
            Some(1)
        );
        assert_eq!(
            date1_decoded.get_field_by_name("day").unwrap().as_i32(),
            Some(15)
        );

        let date2_decoded = list[1].as_message().unwrap();
        assert_eq!(
            date2_decoded.get_field_by_name("year").unwrap().as_i32(),
            Some(2021)
        );
        assert_eq!(
            date2_decoded.get_field_by_name("month").unwrap().as_i32(),
            Some(6)
        );
        assert_eq!(
            date2_decoded.get_field_by_name("day").unwrap().as_i32(),
            Some(30)
        );
    }

    // ==================== TimeOfDay Field Tests ====================

    fn create_time_of_day_pool() -> DescriptorPool {
        let mut pool = DescriptorPool::new();

        // Add google.type.TimeOfDay dependency
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("google/type/timeofday.proto".to_string()),
            package: Some("google.type".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some("TimeOfDay".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("hours".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("minutes".to_string()),
                        number: Some(2),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("seconds".to_string()),
                        number: Some(3),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("nanos".to_string()),
                        number: Some(4),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        pool
    }

    #[test]
    fn test_time_of_day_field_roundtrip() {
        let mut pool = create_time_of_day_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/type/timeofday.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithTimeOfDay".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("time".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.type.TimeOfDay".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithTimeOfDay").unwrap();
        let time_descriptor = pool.get_message_by_name("google.type.TimeOfDay").unwrap();

        let mut time = DynamicMessage::new(time_descriptor.clone());
        time.set_field_by_name("hours", Value::I32(14));
        time.set_field_by_name("minutes", Value::I32(30));
        time.set_field_by_name("seconds", Value::I32(45));
        time.set_field_by_name("nanos", Value::I32(123456789));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("time", Value::Message(time));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let time_value = decoded.get_field_by_name("time").unwrap();
        let time_msg = time_value.as_message().unwrap();
        assert_eq!(
            time_msg.get_field_by_name("hours").unwrap().as_i32(),
            Some(14)
        );
        assert_eq!(
            time_msg.get_field_by_name("minutes").unwrap().as_i32(),
            Some(30)
        );
        assert_eq!(
            time_msg.get_field_by_name("seconds").unwrap().as_i32(),
            Some(45)
        );
        assert_eq!(
            time_msg.get_field_by_name("nanos").unwrap().as_i32(),
            Some(123456789)
        );
    }

    #[test]
    fn test_repeated_time_of_day_roundtrip() {
        let mut pool = create_time_of_day_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/type/timeofday.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("WithTimes".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("times".to_string()),
                    number: Some(1),
                    label: Some(Label::Repeated.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.type.TimeOfDay".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithTimes").unwrap();
        let time_descriptor = pool.get_message_by_name("google.type.TimeOfDay").unwrap();

        let mut time1 = DynamicMessage::new(time_descriptor.clone());
        time1.set_field_by_name("hours", Value::I32(9));
        time1.set_field_by_name("minutes", Value::I32(0));
        time1.set_field_by_name("seconds", Value::I32(0));
        time1.set_field_by_name("nanos", Value::I32(0));

        let mut time2 = DynamicMessage::new(time_descriptor.clone());
        time2.set_field_by_name("hours", Value::I32(17));
        time2.set_field_by_name("minutes", Value::I32(30));
        time2.set_field_by_name("seconds", Value::I32(59));
        time2.set_field_by_name("nanos", Value::I32(999999999));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "times",
            Value::List(vec![Value::Message(time1), Value::Message(time2)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let times_list = decoded.get_field_by_name("times").unwrap();
        let list = times_list.as_list().unwrap();
        assert_eq!(list.len(), 2);

        let time1_decoded = list[0].as_message().unwrap();
        assert_eq!(
            time1_decoded.get_field_by_name("hours").unwrap().as_i32(),
            Some(9)
        );
        assert_eq!(
            time1_decoded.get_field_by_name("minutes").unwrap().as_i32(),
            Some(0)
        );
        assert_eq!(
            time1_decoded.get_field_by_name("seconds").unwrap().as_i32(),
            Some(0)
        );
        assert_eq!(
            time1_decoded.get_field_by_name("nanos").unwrap().as_i32(),
            Some(0)
        );

        let time2_decoded = list[1].as_message().unwrap();
        assert_eq!(
            time2_decoded.get_field_by_name("hours").unwrap().as_i32(),
            Some(17)
        );
        assert_eq!(
            time2_decoded.get_field_by_name("minutes").unwrap().as_i32(),
            Some(30)
        );
        assert_eq!(
            time2_decoded.get_field_by_name("seconds").unwrap().as_i32(),
            Some(59)
        );
        assert_eq!(
            time2_decoded.get_field_by_name("nanos").unwrap().as_i32(),
            Some(999999999)
        );
    }

    // ==================== Enum Field Tests ====================

    fn create_enum_message_descriptor() -> (DescriptorPool, MessageDescriptor) {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            enum_type: vec![prost_reflect::prost_types::EnumDescriptorProto {
                name: Some("Status".to_string()),
                value: vec![
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("UNKNOWN".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("ACTIVE".to_string()),
                        number: Some(1),
                        ..Default::default()
                    },
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("INACTIVE".to_string()),
                        number: Some(2),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            message_type: vec![DescriptorProto {
                name: Some("WithEnum".to_string()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("status".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Enum.into()),
                        type_name: Some(".test.Status".to_string()),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("statuses".to_string()),
                        number: Some(2),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Enum.into()),
                        type_name: Some(".test.Status".to_string()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        };
        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.WithEnum").unwrap();
        (pool, message_descriptor)
    }

    #[test]
    fn test_enum_field_roundtrip() {
        let (_pool, message_descriptor) = create_enum_message_descriptor();

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("status", Value::EnumNumber(1)); // ACTIVE

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        assert_eq!(
            decoded
                .get_field_by_name("status")
                .unwrap()
                .as_enum_number(),
            Some(1)
        );
    }

    #[test]
    fn test_repeated_enum_field_roundtrip() {
        let (_pool, message_descriptor) = create_enum_message_descriptor();

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "statuses",
            Value::List(vec![
                Value::EnumNumber(0), // UNKNOWN
                Value::EnumNumber(1), // ACTIVE
                Value::EnumNumber(2), // INACTIVE
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let statuses = decoded.get_field_by_name("statuses").unwrap();
        let list = statuses.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_enum_number(), Some(0));
        assert_eq!(list[1].as_enum_number(), Some(1));
        assert_eq!(list[2].as_enum_number(), Some(2));
    }

    // ==================== Map Field Tests with Various Key Types ====================

    fn create_map_descriptor(
        key_type: Type,
        value_type: Type,
    ) -> (DescriptorPool, MessageDescriptor) {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("MapEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(key_type.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(value_type.into()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("MessageWithMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("my_map".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.MapEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.MessageWithMap").unwrap();
        (pool, message_descriptor)
    }

    #[test]
    fn test_map_int32_key_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::Int32, Type::String);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::I32(1),
            Value::String("one".to_string()),
        );
        map.insert(
            prost_reflect::MapKey::I32(2),
            Value::String("two".to_string()),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_int64_key_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::Int64, Type::Int32);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(prost_reflect::MapKey::I64(100), Value::I32(1000));
        map.insert(prost_reflect::MapKey::I64(200), Value::I32(2000));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_uint32_key_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::Uint32, Type::Double);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(prost_reflect::MapKey::U32(10), Value::F64(1.5));
        map.insert(prost_reflect::MapKey::U32(20), Value::F64(2.5));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_uint64_key_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::Uint64, Type::Float);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(prost_reflect::MapKey::U64(1000), Value::F32(1.1));
        map.insert(prost_reflect::MapKey::U64(2000), Value::F32(2.2));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_bool_key_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::Bool, Type::Int64);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(prost_reflect::MapKey::Bool(true), Value::I64(100));
        map.insert(prost_reflect::MapKey::Bool(false), Value::I64(200));

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_with_bool_value_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::String, Type::Bool);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("enabled".to_string()),
            Value::Bool(true),
        );
        map.insert(
            prost_reflect::MapKey::String("disabled".to_string()),
            Value::Bool(false),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_with_bytes_value_roundtrip() {
        use std::collections::HashMap;
        let (_pool, message_descriptor) = create_map_descriptor(Type::String, Type::Bytes);

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("data1".to_string()),
            Value::Bytes(prost::bytes::Bytes::from(vec![1, 2, 3])),
        );
        map.insert(
            prost_reflect::MapKey::String("data2".to_string()),
            Value::Bytes(prost::bytes::Bytes::from(vec![4, 5, 6])),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("my_map", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("my_map").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    // ==================== Repeated sfixed/sint/fixed Types Tests ====================

    #[test]
    fn test_repeated_sfixed32_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Sfixed32);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![Value::I32(-100), Value::I32(0), Value::I32(100)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_i32(), Some(-100));
        assert_eq!(list[1].as_i32(), Some(0));
        assert_eq!(list[2].as_i32(), Some(100));
    }

    #[test]
    fn test_repeated_sfixed64_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Sfixed64);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::I64(i64::MIN),
                Value::I64(0),
                Value::I64(i64::MAX),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_i64(), Some(i64::MIN));
        assert_eq!(list[1].as_i64(), Some(0));
        assert_eq!(list[2].as_i64(), Some(i64::MAX));
    }

    #[test]
    fn test_repeated_sint32_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Sint32);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::I32(i32::MIN),
                Value::I32(-1),
                Value::I32(0),
                Value::I32(1),
                Value::I32(i32::MAX),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 5);
        assert_eq!(list[0].as_i32(), Some(i32::MIN));
        assert_eq!(list[1].as_i32(), Some(-1));
        assert_eq!(list[2].as_i32(), Some(0));
        assert_eq!(list[3].as_i32(), Some(1));
        assert_eq!(list[4].as_i32(), Some(i32::MAX));
    }

    #[test]
    fn test_repeated_sint64_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Sint64);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::I64(i64::MIN),
                Value::I64(-1),
                Value::I64(0),
                Value::I64(1),
                Value::I64(i64::MAX),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 5);
        assert_eq!(list[0].as_i64(), Some(i64::MIN));
        assert_eq!(list[1].as_i64(), Some(-1));
        assert_eq!(list[2].as_i64(), Some(0));
        assert_eq!(list[3].as_i64(), Some(1));
        assert_eq!(list[4].as_i64(), Some(i64::MAX));
    }

    #[test]
    fn test_repeated_fixed32_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Fixed32);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![Value::U32(0), Value::U32(100), Value::U32(u32::MAX)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_u32(), Some(0));
        assert_eq!(list[1].as_u32(), Some(100));
        assert_eq!(list[2].as_u32(), Some(u32::MAX));
    }

    #[test]
    fn test_repeated_fixed64_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Fixed64);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![Value::U64(0), Value::U64(100), Value::U64(u64::MAX)]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].as_u64(), Some(0));
        assert_eq!(list[1].as_u64(), Some(100));
        assert_eq!(list[2].as_u64(), Some(u64::MAX));
    }

    // ==================== Additional Coverage Tests ====================

    #[test]
    fn test_repeated_double_roundtrip() {
        let (_pool, message_descriptor) =
            create_repeated_message_descriptor("values", Type::Double);

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name(
            "values",
            Value::List(vec![
                Value::F64(-std::f64::consts::PI),
                Value::F64(0.0),
                Value::F64(std::f64::consts::E),
            ]),
        );

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let values = decoded.get_field_by_name("values").unwrap();
        let list = values.as_list().unwrap();
        assert_eq!(list.len(), 3);
        assert!((list[0].as_f64().unwrap() + std::f64::consts::PI).abs() < 1e-10);
        assert!((list[1].as_f64().unwrap()).abs() < 1e-10);
        assert!((list[2].as_f64().unwrap() - std::f64::consts::E).abs() < 1e-10);
    }

    #[test]
    fn test_null_nested_message_roundtrip() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Inner".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("value".to_string()),
                        number: Some(1),
                        label: Some(Label::Optional.into()),
                        r#type: Some(Type::Int32.into()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Outer".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("id".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("inner".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".test.Inner".to_string()),
                            proto3_optional: Some(true),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let outer_descriptor = pool.get_message_by_name("test.Outer").unwrap();

        // Create message without setting the inner field (it will be null)
        let mut msg = DynamicMessage::new(outer_descriptor.clone());
        msg.set_field_by_name("id", Value::I32(42));
        // Don't set inner - it should be null

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &outer_descriptor);
        let array_data = record_batch_to_array(&record_batch, &outer_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(outer_descriptor.clone(), binary_array.value(0)).unwrap();

        assert_eq!(decoded.get_field_by_name("id").unwrap().as_i32(), Some(42));
    }

    #[test]
    fn test_multiple_rows_with_different_repeated_lengths() {
        let (_pool, message_descriptor) = create_repeated_message_descriptor("values", Type::Int32);

        let mut msg1 = DynamicMessage::new(message_descriptor.clone());
        msg1.set_field_by_name("values", Value::List(vec![Value::I32(1)]));

        let mut msg2 = DynamicMessage::new(message_descriptor.clone());
        msg2.set_field_by_name(
            "values",
            Value::List(vec![Value::I32(1), Value::I32(2), Value::I32(3)]),
        );

        let mut msg3 = DynamicMessage::new(message_descriptor.clone());
        msg3.set_field_by_name("values", Value::List(vec![]));

        let messages = vec![msg1, msg2, msg3];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        assert_eq!(binary_array.len(), 3);

        let decoded1 =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();
        let values1 = decoded1.get_field_by_name("values").unwrap();
        let list1 = values1.as_list().unwrap();
        assert_eq!(list1.len(), 1);

        let decoded2 =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(1)).unwrap();
        let values2 = decoded2.get_field_by_name("values").unwrap();
        let list2 = values2.as_list().unwrap();
        assert_eq!(list2.len(), 3);

        let decoded3 =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(2)).unwrap();
        let values3 = decoded3.get_field_by_name("values").unwrap();
        let list3 = values3.as_list().unwrap();
        assert_eq!(list3.len(), 0);
    }

    #[test]
    fn test_map_with_enum_value_roundtrip() {
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            enum_type: vec![prost_reflect::prost_types::EnumDescriptorProto {
                name: Some("Priority".to_string()),
                value: vec![
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("LOW".to_string()),
                        number: Some(0),
                        ..Default::default()
                    },
                    prost_reflect::prost_types::EnumValueDescriptorProto {
                        name: Some("HIGH".to_string()),
                        number: Some(1),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            message_type: vec![
                DescriptorProto {
                    name: Some("PriorityEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Enum.into()),
                            type_name: Some(".test.Priority".to_string()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("WithEnumMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("priorities".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.PriorityEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        use std::collections::HashMap;
        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.WithEnumMap").unwrap();

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("task1".to_string()),
            Value::EnumNumber(0), // LOW
        );
        map.insert(
            prost_reflect::MapKey::String("task2".to_string()),
            Value::EnumNumber(1), // HIGH
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("priorities", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("priorities").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);
    }

    // ==================== Map with Message Value Tests ====================

    fn create_wrapper_pool(wrapper_name: &str) -> DescriptorPool {
        let mut pool = DescriptorPool::new();
        let (package, field_type) = match wrapper_name {
            "DoubleValue" => ("google.protobuf", Type::Double),
            "FloatValue" => ("google.protobuf", Type::Float),
            "Int64Value" => ("google.protobuf", Type::Int64),
            "UInt64Value" => ("google.protobuf", Type::Uint64),
            "Int32Value" => ("google.protobuf", Type::Int32),
            "UInt32Value" => ("google.protobuf", Type::Uint32),
            "BoolValue" => ("google.protobuf", Type::Bool),
            "StringValue" => ("google.protobuf", Type::String),
            "BytesValue" => ("google.protobuf", Type::Bytes),
            _ => panic!("Unknown wrapper type"),
        };
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some(format!("google/protobuf/wrappers.proto")),
            package: Some(package.to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![DescriptorProto {
                name: Some(wrapper_name.to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("value".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(field_type.into()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        })
        .unwrap();
        pool
    }

    #[test]
    fn test_map_with_timestamp_value_roundtrip() {
        use std::collections::HashMap;
        let mut pool = create_timestamp_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/protobuf/timestamp.proto".to_string()],
            message_type: vec![
                DescriptorProto {
                    name: Some("TimestampEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".google.protobuf.Timestamp".to_string()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("WithTimestampMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("timestamps".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.TimestampEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithTimestampMap").unwrap();
        let timestamp_descriptor = pool
            .get_message_by_name("google.protobuf.Timestamp")
            .unwrap();

        let mut ts1 = DynamicMessage::new(timestamp_descriptor.clone());
        ts1.set_field_by_name("seconds", Value::I64(1700000000));
        ts1.set_field_by_name("nanos", Value::I32(123456789));

        let mut ts2 = DynamicMessage::new(timestamp_descriptor.clone());
        ts2.set_field_by_name("seconds", Value::I64(1600000000));
        ts2.set_field_by_name("nanos", Value::I32(0));

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("event1".to_string()),
            Value::Message(ts1),
        );
        map.insert(
            prost_reflect::MapKey::String("event2".to_string()),
            Value::Message(ts2),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("timestamps", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("timestamps").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);

        // Check that we can access the timestamp values
        for (key, value) in map {
            let ts_msg = value.as_message().unwrap();
            let seconds = ts_msg
                .get_field_by_name("seconds")
                .unwrap()
                .as_i64()
                .unwrap();
            let nanos = ts_msg.get_field_by_name("nanos").unwrap().as_i32().unwrap();
            match key.as_str().unwrap() {
                "event1" => {
                    assert_eq!(seconds, 1700000000);
                    assert_eq!(nanos, 123456789);
                }
                "event2" => {
                    assert_eq!(seconds, 1600000000);
                    assert_eq!(nanos, 0);
                }
                _ => panic!("Unexpected key"),
            }
        }
    }

    #[test]
    fn test_map_with_date_value_roundtrip() {
        use std::collections::HashMap;
        let mut pool = create_date_pool();
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/type/date.proto".to_string()],
            message_type: vec![
                DescriptorProto {
                    name: Some("DateEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".google.type.Date".to_string()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("WithDateMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("dates".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.DateEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithDateMap").unwrap();
        let date_descriptor = pool.get_message_by_name("google.type.Date").unwrap();

        let mut date1 = DynamicMessage::new(date_descriptor.clone());
        date1.set_field_by_name("year", Value::I32(2024));
        date1.set_field_by_name("month", Value::I32(12));
        date1.set_field_by_name("day", Value::I32(25));

        let mut date2 = DynamicMessage::new(date_descriptor.clone());
        date2.set_field_by_name("year", Value::I32(2023));
        date2.set_field_by_name("month", Value::I32(1));
        date2.set_field_by_name("day", Value::I32(1));

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("christmas".to_string()),
            Value::Message(date1),
        );
        map.insert(
            prost_reflect::MapKey::String("new_year".to_string()),
            Value::Message(date2),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("dates", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("dates").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);

        for (key, value) in map {
            let date_msg = value.as_message().unwrap();
            let year = date_msg
                .get_field_by_name("year")
                .unwrap()
                .as_i32()
                .unwrap();
            let month = date_msg
                .get_field_by_name("month")
                .unwrap()
                .as_i32()
                .unwrap();
            let day = date_msg.get_field_by_name("day").unwrap().as_i32().unwrap();
            match key.as_str().unwrap() {
                "christmas" => {
                    assert_eq!(year, 2024);
                    assert_eq!(month, 12);
                    assert_eq!(day, 25);
                }
                "new_year" => {
                    assert_eq!(year, 2023);
                    assert_eq!(month, 1);
                    assert_eq!(day, 1);
                }
                _ => panic!("Unexpected key"),
            }
        }
    }

    #[test]
    fn test_map_with_double_value_roundtrip() {
        use std::collections::HashMap;
        let mut pool = create_wrapper_pool("DoubleValue");
        pool.add_file_descriptor_proto(prost_reflect::prost_types::FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/protobuf/wrappers.proto".to_string()],
            message_type: vec![
                DescriptorProto {
                    name: Some("DoubleEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".google.protobuf.DoubleValue".to_string()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("WithDoubleValueMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("values".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.DoubleEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        })
        .unwrap();

        let message_descriptor = pool.get_message_by_name("test.WithDoubleValueMap").unwrap();
        let wrapper_descriptor = pool
            .get_message_by_name("google.protobuf.DoubleValue")
            .unwrap();

        let mut val1 = DynamicMessage::new(wrapper_descriptor.clone());
        val1.set_field_by_name("value", Value::F64(3.14159));

        let mut val2 = DynamicMessage::new(wrapper_descriptor.clone());
        val2.set_field_by_name("value", Value::F64(2.71828));

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("pi".to_string()),
            Value::Message(val1),
        );
        map.insert(
            prost_reflect::MapKey::String("e".to_string()),
            Value::Message(val2),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("values", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("values").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);

        for (key, value) in map {
            let wrapper_msg = value.as_message().unwrap();
            let val = wrapper_msg
                .get_field_by_name("value")
                .unwrap()
                .as_f64()
                .unwrap();
            match key.as_str().unwrap() {
                "pi" => assert!((val - 3.14159).abs() < 1e-5),
                "e" => assert!((val - 2.71828).abs() < 1e-5),
                _ => panic!("Unexpected key"),
            }
        }
    }

    #[test]
    fn test_map_with_nested_message_value_roundtrip() {
        use std::collections::HashMap;
        let file_descriptor = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Point".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("x".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("y".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Int32.into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("PointEntry".to_string()),
                    field: vec![
                        FieldDescriptorProto {
                            name: Some("key".to_string()),
                            number: Some(1),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::String.into()),
                            ..Default::default()
                        },
                        FieldDescriptorProto {
                            name: Some("value".to_string()),
                            number: Some(2),
                            label: Some(Label::Optional.into()),
                            r#type: Some(Type::Message.into()),
                            type_name: Some(".test.Point".to_string()),
                            ..Default::default()
                        },
                    ],
                    options: Some(prost_reflect::prost_types::MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("WithPointMap".to_string()),
                    field: vec![FieldDescriptorProto {
                        name: Some("points".to_string()),
                        number: Some(1),
                        label: Some(Label::Repeated.into()),
                        r#type: Some(Type::Message.into()),
                        type_name: Some(".test.PointEntry".to_string()),
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let pool = create_pool_with_message(file_descriptor);
        let message_descriptor = pool.get_message_by_name("test.WithPointMap").unwrap();
        let point_descriptor = pool.get_message_by_name("test.Point").unwrap();

        let mut point1 = DynamicMessage::new(point_descriptor.clone());
        point1.set_field_by_name("x", Value::I32(10));
        point1.set_field_by_name("y", Value::I32(20));

        let mut point2 = DynamicMessage::new(point_descriptor.clone());
        point2.set_field_by_name("x", Value::I32(30));
        point2.set_field_by_name("y", Value::I32(40));

        let mut map: HashMap<prost_reflect::MapKey, Value> = HashMap::new();
        map.insert(
            prost_reflect::MapKey::String("origin".to_string()),
            Value::Message(point1),
        );
        map.insert(
            prost_reflect::MapKey::String("destination".to_string()),
            Value::Message(point2),
        );

        let mut msg = DynamicMessage::new(message_descriptor.clone());
        msg.set_field_by_name("points", Value::Map(map));

        let messages = vec![msg];
        let record_batch = messages_to_record_batch(&messages, &message_descriptor);
        let array_data = record_batch_to_array(&record_batch, &message_descriptor);

        let binary_array = arrow::array::BinaryArray::from(array_data);
        let decoded =
            DynamicMessage::decode(message_descriptor.clone(), binary_array.value(0)).unwrap();

        let map_value = decoded.get_field_by_name("points").unwrap();
        let map = map_value.as_map().unwrap();
        assert_eq!(map.len(), 2);

        for (key, value) in map {
            let point_msg = value.as_message().unwrap();
            let x = point_msg.get_field_by_name("x").unwrap().as_i32().unwrap();
            let y = point_msg.get_field_by_name("y").unwrap().as_i32().unwrap();
            match key.as_str().unwrap() {
                "origin" => {
                    assert_eq!(x, 10);
                    assert_eq!(y, 20);
                }
                "destination" => {
                    assert_eq!(x, 30);
                    assert_eq!(y, 40);
                }
                _ => panic!("Unexpected key"),
            }
        }
    }

    // ==================== Binary Array Conversion Tests ====================

    #[test]
    fn test_binary_array_to_messages() {
        use crate::proto_to_arrow::binary_array_to_messages;
        use arrow_array::BinaryArray;
        use prost::Message;

        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        // Create some messages and serialize them
        let messages = dynamic_messages_fixture(&message_descriptor);
        let serialized: Vec<Vec<u8>> = messages.iter().map(|m| m.encode_to_vec()).collect();

        // Create a binary array from the serialized messages
        let binary_array = BinaryArray::from_iter_values(serialized.iter().map(|v| v.as_slice()));

        // Convert back to messages
        let decoded_messages =
            binary_array_to_messages(&binary_array, &message_descriptor).unwrap();

        assert_eq!(decoded_messages.len(), 2);
        assert_eq!(
            decoded_messages[0]
                .get_field_by_name("id")
                .unwrap()
                .as_i32()
                .unwrap(),
            1
        );
        assert_eq!(
            decoded_messages[0]
                .get_field_by_name("name")
                .unwrap()
                .as_str()
                .unwrap(),
            "test"
        );
        assert_eq!(
            decoded_messages[1]
                .get_field_by_name("id")
                .unwrap()
                .as_i32()
                .unwrap(),
            2
        );
        assert_eq!(
            decoded_messages[1]
                .get_field_by_name("name")
                .unwrap()
                .as_str()
                .unwrap(),
            "test2"
        );
    }

    #[test]
    fn test_binary_array_to_messages_with_nulls() {
        use crate::proto_to_arrow::binary_array_to_messages;
        use arrow_array::BinaryArray;
        use prost::Message;

        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        // Create a message and serialize it
        let mut message = DynamicMessage::new(message_descriptor.clone());
        message.set_field_by_name("id", prost_reflect::Value::I32(42));
        message.set_field_by_name("name", prost_reflect::Value::String("hello".to_string()));
        let serialized = message.encode_to_vec();

        // Create a binary array with a null value
        let binary_array = BinaryArray::from_iter(vec![
            Some(serialized.as_slice()),
            None,
            Some(serialized.as_slice()),
        ]);

        // Convert back to messages
        let decoded_messages =
            binary_array_to_messages(&binary_array, &message_descriptor).unwrap();

        assert_eq!(decoded_messages.len(), 3);
        // First message
        assert_eq!(
            decoded_messages[0]
                .get_field_by_name("id")
                .unwrap()
                .as_i32()
                .unwrap(),
            42
        );
        // Null becomes default message
        assert_eq!(
            decoded_messages[1]
                .get_field_by_name("id")
                .unwrap()
                .as_i32()
                .unwrap(),
            0
        );
        // Third message
        assert_eq!(
            decoded_messages[2]
                .get_field_by_name("id")
                .unwrap()
                .as_i32()
                .unwrap(),
            42
        );
    }

    #[test]
    fn test_binary_array_to_record_batch() {
        use crate::proto_to_arrow::binary_array_to_record_batch;
        use arrow_array::BinaryArray;
        use prost::Message;

        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        // Create some messages and serialize them
        let messages = dynamic_messages_fixture(&message_descriptor);
        let serialized: Vec<Vec<u8>> = messages.iter().map(|m| m.encode_to_vec()).collect();

        // Create a binary array from the serialized messages
        let binary_array = BinaryArray::from_iter_values(serialized.iter().map(|v| v.as_slice()));

        // Convert to record batch
        let record_batch =
            binary_array_to_record_batch(&binary_array, &message_descriptor).unwrap();

        assert_eq!(record_batch.num_rows(), 2);
        assert_eq!(record_batch.num_columns(), 2);

        // Check column names
        assert_eq!(record_batch.schema().field(0).name(), "id");
        assert_eq!(record_batch.schema().field(1).name(), "name");

        // Check values
        let id_array = record_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 2);

        let name_array = record_batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(name_array.value(0), "test");
        assert_eq!(name_array.value(1), "test2");
    }

    #[test]
    fn test_binary_array_roundtrip() {
        use crate::proto_to_arrow::binary_array_to_record_batch;
        use arrow_array::BinaryArray;
        use prost::Message;

        let file_descriptor_proto = file_descriptor_proto_fixture();
        let pool = create_pool_with_message(file_descriptor_proto);
        let message_descriptor = pool.get_message_by_name("test.TestMessage").unwrap();

        // Create some messages and serialize them
        let original_messages = dynamic_messages_fixture(&message_descriptor);
        let serialized: Vec<Vec<u8>> = original_messages
            .iter()
            .map(|m| m.encode_to_vec())
            .collect();

        // Binary array -> Record batch -> Binary array -> Messages
        let binary_array = BinaryArray::from_iter_values(serialized.iter().map(|v| v.as_slice()));
        let record_batch =
            binary_array_to_record_batch(&binary_array, &message_descriptor).unwrap();
        let result_array = record_batch_to_array(&record_batch, &message_descriptor);
        let result_binary = arrow::array::BinaryArray::from(result_array);

        // Decode and compare
        for i in 0..result_binary.len() {
            let decoded =
                DynamicMessage::decode(message_descriptor.clone(), result_binary.value(i)).unwrap();
            let original = &original_messages[i];
            assert_eq!(
                decoded.get_field_by_name("id").unwrap().as_i32().unwrap(),
                original.get_field_by_name("id").unwrap().as_i32().unwrap()
            );
            assert_eq!(
                decoded.get_field_by_name("name").unwrap().as_str().unwrap(),
                original
                    .get_field_by_name("name")
                    .unwrap()
                    .as_str()
                    .unwrap()
            );
        }
    }
}
