//! Proves that ptars can exchange Arrow data with a consumer compiled against
//! a different arrow major version, via the Arrow C Data Interface.
//!
//! This crate pins arrow 58 while ptars internally uses arrow 59 (see the two
//! Cargo.toml files). If ptars ever leaked arrow types in its public API, or
//! broke the C Data Interface contract, these tests would fail to compile or
//! fail at runtime.

#[cfg(test)]
mod tests {
    use arrow::array::{Array, AsArray, BinaryArray, StructArray};
    use arrow::datatypes::Int32Type;
    use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use arrow::record_batch::RecordBatch;
    use prost::Message;
    use prost_types::{
        field_descriptor_proto::{Label, Type},
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    use ptars::{Handler, PtarsConfig};

    // Both the consumer's arrow and ptars::ffi implement the same frozen
    // C ABI (checked by compile-time layout assertions inside ptars), so the
    // structs can be transmuted into one another.
    fn export(
        array: FFI_ArrowArray,
        schema: FFI_ArrowSchema,
    ) -> (ptars::ffi::ArrowArray, ptars::ffi::ArrowSchema) {
        unsafe {
            (
                std::mem::transmute::<FFI_ArrowArray, ptars::ffi::ArrowArray>(array),
                std::mem::transmute::<FFI_ArrowSchema, ptars::ffi::ArrowSchema>(schema),
            )
        }
    }

    fn import(
        array: ptars::ffi::ArrowArray,
        schema: ptars::ffi::ArrowSchema,
    ) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        unsafe {
            (
                std::mem::transmute::<ptars::ffi::ArrowArray, FFI_ArrowArray>(array),
                std::mem::transmute::<ptars::ffi::ArrowSchema, FFI_ArrowSchema>(schema),
            )
        }
    }

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

    // test.TestMessage { id: 150, name: "abc" } in protobuf wire format.
    const MESSAGE_1: &[u8] = b"\x08\x96\x01\x12\x03abc";
    // test.TestMessage { id: 1, name: "d" }
    const MESSAGE_2: &[u8] = b"\x08\x01\x12\x01d";

    #[test]
    fn test_round_trip_across_arrow_versions() {
        let handler = Handler::try_new(
            &descriptor_set_bytes(),
            "test.TestMessage",
            PtarsConfig::default(),
        )
        .unwrap();

        // Produce the input with the consumer's arrow (58).
        let binary = BinaryArray::from(vec![Some(MESSAGE_1), Some(MESSAGE_2)]);
        let (ffi_array, ffi_schema) = to_ffi(&binary.to_data()).unwrap();
        let (array, schema) = export(ffi_array, ffi_schema);

        // Decode through ptars (built against arrow 59).
        let (out_array, out_schema) = handler.decode(array, &schema).unwrap();

        // Import the result back with arrow 58 and check the values.
        let (ffi_array, ffi_schema) = import(out_array, out_schema);
        let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let batch: RecordBatch = StructArray::from(data).into();

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

        // And encode back to protobuf, again crossing the version boundary.
        let struct_array = StructArray::from(batch);
        let (ffi_array, ffi_schema) = to_ffi(&struct_array.to_data()).unwrap();
        let (array, schema) = export(ffi_array, ffi_schema);
        let (encoded_array, encoded_schema) = handler.encode(array, &schema).unwrap();

        let (ffi_array, ffi_schema) = import(encoded_array, encoded_schema);
        let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let encoded = BinaryArray::from(data);
        assert_eq!(encoded.value(0), MESSAGE_1);
        assert_eq!(encoded.value(1), MESSAGE_2);
    }

    #[test]
    fn test_decode_bytes_without_arrow_input() {
        let handler = Handler::try_new(
            &descriptor_set_bytes(),
            "test.TestMessage",
            PtarsConfig::default(),
        )
        .unwrap();

        let (array, schema) = handler.decode_bytes(&[Some(MESSAGE_1), None]).unwrap();
        let (ffi_array, ffi_schema) = import(array, schema);
        let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let batch: RecordBatch = StructArray::from(data).into();
        assert_eq!(batch.num_rows(), 2);
    }
}
