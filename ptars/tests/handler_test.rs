use arrow::array::{Array, AsArray};
use arrow::datatypes::Int32Type;
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{BinaryArray, RecordBatch, StructArray};
use prost::Message;
use prost_types::{
    field_descriptor_proto::{Label, Type},
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use ptars::{Handler, PtarsConfig};

fn descriptor_set_bytes() -> Vec<u8> {
    let file = FileDescriptorProto {
        name: Some("test.proto".to_string()),
        package: Some("test".to_string()),
        syntax: Some("proto3".to_string()),
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
    };
    FileDescriptorSet { file: vec![file] }.encode_to_vec()
}

fn handler() -> Handler {
    Handler::try_new(
        &descriptor_set_bytes(),
        "test.TestMessage",
        PtarsConfig::default(),
    )
    .unwrap()
}

// test.TestMessage { id: 150, name: "abc" } in protobuf wire format.
const MESSAGE_1: &[u8] = b"\x08\x96\x01\x12\x03abc";
// test.TestMessage { id: 1, name: "d" }
const MESSAGE_2: &[u8] = b"\x08\x01\x12\x01d";

fn import_record_batch(
    array: ptars::ffi::ArrowArray,
    schema: ptars::ffi::ArrowSchema,
) -> RecordBatch {
    // SAFETY: ptars::ffi structs implement the same frozen C ABI as arrow's.
    let ffi_array: FFI_ArrowArray = unsafe { std::mem::transmute(array) };
    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
    let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
    StructArray::from(data).into()
}

#[test]
fn test_decode_bytes_round_trip() {
    let handler = handler();
    let (array, schema) = handler
        .decode_bytes(&[Some(MESSAGE_1), Some(MESSAGE_2)])
        .unwrap();
    let batch = import_record_batch(array, schema);

    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int32Type>();
    assert_eq!(ids.value(0), 150);
    assert_eq!(ids.value(1), 1);
    let names = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(names.value(0), "abc");
    assert_eq!(names.value(1), "d");
}

#[test]
fn test_decode_from_ffi_binary_array() {
    let handler = handler();
    let binary = BinaryArray::from(vec![Some(MESSAGE_1), Some(MESSAGE_2)]);
    let (ffi_array, ffi_schema) = to_ffi(&binary.to_data()).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };

    let (out_array, out_schema) = handler.decode(array, &schema).unwrap();
    let batch = import_record_batch(out_array, out_schema);
    assert_eq!(batch.num_rows(), 2);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int32Type>();
    assert_eq!(ids.value(0), 150);
}

#[test]
fn test_encode_round_trip() {
    let handler = handler();
    let (array, schema) = handler
        .decode_bytes(&[Some(MESSAGE_1), Some(MESSAGE_2)])
        .unwrap();

    let (encoded_array, encoded_schema) = handler.encode(array, &schema).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let ffi_array: FFI_ArrowArray = unsafe { std::mem::transmute(encoded_array) };
    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute(encoded_schema) };
    let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
    let binary = BinaryArray::from(data);

    assert_eq!(binary.len(), 2);
    assert_eq!(binary.value(0), MESSAGE_1);
    assert_eq!(binary.value(1), MESSAGE_2);
}

#[test]
fn test_arrow_schema() {
    let handler = handler();
    let schema = handler.arrow_schema().unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
    let imported = arrow_schema::Schema::try_from(&ffi_schema).unwrap();
    assert_eq!(imported.fields().len(), 2);
    assert_eq!(imported.field(0).name(), "id");
    assert_eq!(imported.field(1).name(), "name");
}

#[test]
fn test_decode_rejects_non_binary() {
    let handler = handler();
    let ints = arrow_array::Int32Array::from(vec![1, 2, 3]);
    let (ffi_array, ffi_schema) = to_ffi(&ints.to_data()).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };
    let result = handler.decode(array, &schema);
    assert!(matches!(result, Err(ptars::PtarsError::Decode(_))));
}

#[test]
fn test_unknown_message_name() {
    let result = Handler::try_new(
        &descriptor_set_bytes(),
        "test.DoesNotExist",
        PtarsConfig::default(),
    );
    assert!(matches!(result, Err(ptars::PtarsError::Descriptor(_))));
}

/// google/protobuf/timestamp.proto and a test file importing it, as separate
/// FileDescriptorProto messages (import order matters for
/// try_new_from_file_descriptor_protos).
fn timestamp_file_descriptor_protos() -> (Vec<u8>, Vec<u8>) {
    let timestamp_file = FileDescriptorProto {
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
    };
    let main_file = FileDescriptorProto {
        name: Some("test_ts.proto".to_string()),
        package: Some("test".to_string()),
        syntax: Some("proto3".to_string()),
        dependency: vec!["google/protobuf/timestamp.proto".to_string()],
        message_type: vec![DescriptorProto {
            name: Some("WithTimestamp".to_string()),
            field: vec![
                FieldDescriptorProto {
                    name: Some("ts".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional.into()),
                    r#type: Some(Type::Message.into()),
                    type_name: Some(".google.protobuf.Timestamp".to_string()),
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
    };
    (timestamp_file.encode_to_vec(), main_file.encode_to_vec())
}

fn import_schema(schema: ptars::ffi::ArrowSchema) -> arrow_schema::Schema {
    // SAFETY: same frozen C ABI on both sides.
    let ffi_schema: FFI_ArrowSchema = unsafe { std::mem::transmute(schema) };
    arrow_schema::Schema::try_from(&ffi_schema).unwrap()
}

#[test]
fn test_try_new_from_file_descriptor_protos() {
    let (timestamp_file, main_file) = timestamp_file_descriptor_protos();
    let handler = Handler::try_new_from_file_descriptor_protos(
        &[&timestamp_file, &main_file],
        "test.WithTimestamp",
        PtarsConfig::default(),
    )
    .unwrap();

    let schema = import_schema(handler.arrow_schema().unwrap());
    assert_eq!(schema.field(0).name(), "ts");
    assert_eq!(schema.field(1).name(), "name");
}

#[test]
fn test_try_new_from_file_descriptor_protos_invalid_bytes() {
    let result = Handler::try_new_from_file_descriptor_protos(
        &[b"not a file descriptor proto".as_ref()],
        "test.TestMessage",
        PtarsConfig::default(),
    );
    assert!(matches!(result, Err(ptars::PtarsError::Descriptor(_))));
}

#[test]
fn test_config_propagates_through_facade() {
    use arrow_schema::DataType;

    let (timestamp_file, main_file) = timestamp_file_descriptor_protos();
    let config = PtarsConfig::default()
        .with_timestamp_unit(ptars::TimeUnit::Millisecond)
        .with_use_large_string(true);
    let handler = Handler::try_new_from_file_descriptor_protos(
        &[&timestamp_file, &main_file],
        "test.WithTimestamp",
        config,
    )
    .unwrap();

    let schema = import_schema(handler.arrow_schema().unwrap());
    // ptars::TimeUnit must map to the equivalent arrow time unit.
    assert_eq!(
        schema.field_with_name("ts").unwrap().data_type(),
        &DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, Some("UTC".into()))
    );
    assert_eq!(
        schema.field_with_name("name").unwrap().data_type(),
        &DataType::LargeUtf8
    );
}

#[test]
fn test_decode_large_binary_input() {
    let handler = handler();
    let large = arrow_array::LargeBinaryArray::from(vec![Some(MESSAGE_1), Some(MESSAGE_2)]);
    let (ffi_array, ffi_schema) = to_ffi(&large.to_data()).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };

    let (out_array, out_schema) = handler.decode(array, &schema).unwrap();
    let batch = import_record_batch(out_array, out_schema);
    let ids = batch
        .column_by_name("id")
        .unwrap()
        .as_primitive::<Int32Type>();
    assert_eq!(ids.value(0), 150);
    assert_eq!(ids.value(1), 1);
}

#[test]
fn test_decode_binary_view_input() {
    let handler = handler();
    let view = arrow_array::BinaryViewArray::from(vec![Some(MESSAGE_1), Some(MESSAGE_2)]);
    let (ffi_array, ffi_schema) = to_ffi(&view.to_data()).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };

    let (out_array, out_schema) = handler.decode(array, &schema).unwrap();
    let batch = import_record_batch(out_array, out_schema);
    let names = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(names.value(0), "abc");
    assert_eq!(names.value(1), "d");
}

#[test]
fn test_encode_rejects_non_struct() {
    let handler = handler();
    let ints = arrow_array::Int32Array::from(vec![1, 2, 3]);
    let (ffi_array, ffi_schema) = to_ffi(&ints.to_data()).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };
    let result = handler.encode(array, &schema);
    assert!(matches!(result, Err(ptars::PtarsError::Encode(_))));
}

#[test]
fn test_encode_rejects_struct_with_top_level_nulls() {
    use arrow::buffer::NullBuffer;
    use arrow_array::ArrayRef;
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    let handler = handler();
    let fields = Fields::from(vec![Field::new("id", DataType::Int32, true)]);
    let columns: Vec<ArrayRef> = vec![Arc::new(arrow_array::Int32Array::from(vec![1, 2]))];
    let nulls = NullBuffer::from(vec![true, false]);
    let struct_array = StructArray::new(fields, columns, Some(nulls));

    let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data()).unwrap();
    // SAFETY: same frozen C ABI on both sides.
    let array: ptars::ffi::ArrowArray = unsafe { std::mem::transmute(ffi_array) };
    let schema: ptars::ffi::ArrowSchema = unsafe { std::mem::transmute(ffi_schema) };
    let result = handler.encode(array, &schema);
    assert!(matches!(result, Err(ptars::PtarsError::Encode(_))));
}

#[test]
fn test_ffi_empty_and_is_released() {
    assert!(ptars::ffi::ArrowArray::empty().is_released());
    assert!(ptars::ffi::ArrowSchema::empty().is_released());

    // Live exports are not released until dropped.
    let handler = handler();
    let (array, schema) = handler.decode_bytes(&[Some(MESSAGE_1)]).unwrap();
    assert!(!array.is_released());
    assert!(!schema.is_released());
    // Dropping unconsumed results must invoke the release callbacks and not
    // crash (leaks would be caught by miri/valgrind, double-frees crash here).
    drop(array);
    drop(schema);
}
