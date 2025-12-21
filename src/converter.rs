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
}
