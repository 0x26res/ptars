use crate::error::PtarsError;
use crate::ffi::{export_array, export_schema, import_array, import_schema_ref};
use crate::ffi::{ArrowArray, ArrowSchema};
use arrow::ffi::{from_ffi, to_ffi, FFI_ArrowSchema};
use arrow_array::builder::BinaryBuilder;
use arrow_array::{Array, BinaryArray, RecordBatch, StructArray};
use arrow_schema::DataType;
use prost::Message;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use ptars_core::PtarsConfig;

/// Converts between serialized protobuf messages and Arrow record batches for
/// a single message type, exchanging Arrow data through the C Data Interface.
///
/// Descriptors are passed as serialized bytes and Arrow data as
/// [`ArrowArray`]/[`ArrowSchema`] pairs, so this type works with any arrow
/// version on the caller's side.
pub struct Handler {
    descriptor: MessageDescriptor,
    config: PtarsConfig,
}

impl Handler {
    /// Create a handler from a serialized `google.protobuf.FileDescriptorSet`
    /// (e.g. the output of `protoc --descriptor_set_out`) and a fully
    /// qualified message name.
    pub fn try_new(
        file_descriptor_set: &[u8],
        message_name: &str,
        config: PtarsConfig,
    ) -> Result<Self, PtarsError> {
        let pool = DescriptorPool::decode(file_descriptor_set)
            .map_err(|e| PtarsError::Descriptor(e.to_string()))?;
        Self::from_pool(pool, message_name, config)
    }

    /// Create a handler from serialized `google.protobuf.FileDescriptorProto`
    /// messages and a fully qualified message name.
    ///
    /// Files must be ordered so that imports appear before the files that
    /// depend on them.
    pub fn try_new_from_file_descriptor_protos(
        file_descriptor_protos: &[&[u8]],
        message_name: &str,
        config: PtarsConfig,
    ) -> Result<Self, PtarsError> {
        let mut pool = DescriptorPool::new();
        for bytes in file_descriptor_protos {
            let proto = prost_reflect::prost_types::FileDescriptorProto::decode(*bytes)
                .map_err(|e| PtarsError::Descriptor(e.to_string()))?;
            pool.add_file_descriptor_proto(proto)
                .map_err(|e| PtarsError::Descriptor(e.to_string()))?;
        }
        Self::from_pool(pool, message_name, config)
    }

    fn from_pool(
        pool: DescriptorPool,
        message_name: &str,
        config: PtarsConfig,
    ) -> Result<Self, PtarsError> {
        let descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
            PtarsError::Descriptor(format!(
                "message '{}' not found in descriptor pool",
                message_name
            ))
        })?;
        Ok(Self { descriptor, config })
    }

    /// The Arrow schema of the record batches produced by [`Handler::decode`],
    /// exported as a C Data Interface struct-typed schema.
    pub fn arrow_schema(&self) -> Result<ArrowSchema, PtarsError> {
        let batch = self.decode_binary_array(&BinaryArray::from(Vec::<Option<&[u8]>>::new()))?;
        let ffi_schema = FFI_ArrowSchema::try_from(batch.schema().as_ref())
            .map_err(|e| PtarsError::Arrow(e.to_string()))?;
        Ok(export_schema(ffi_schema))
    }

    /// Decode an Arrow binary array of serialized protobuf messages into a
    /// record batch, returned as a struct-typed array over the C Data
    /// Interface.
    ///
    /// The input must be a `Binary`, `LargeBinary` or `BinaryView` array where
    /// each value is one serialized message; nulls decode to null rows.
    pub fn decode(
        &self,
        array: ArrowArray,
        schema: &ArrowSchema,
    ) -> Result<(ArrowArray, ArrowSchema), PtarsError> {
        // SAFETY: the caller guarantees the structs describe valid Arrow data,
        // as required by the C Data Interface contract.
        let data = unsafe { from_ffi(import_array(array), import_schema_ref(schema)) }
            .map_err(|e| PtarsError::Arrow(e.to_string()))?;
        let array = arrow_array::make_array(data);
        let binary: BinaryArray = match array.data_type() {
            DataType::Binary => array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("Binary array")
                .clone(),
            DataType::LargeBinary | DataType::BinaryView => {
                let cast = arrow::compute::cast(&array, &DataType::Binary)
                    .map_err(|e| PtarsError::Arrow(e.to_string()))?;
                cast.as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("Binary array")
                    .clone()
            }
            other => {
                return Err(PtarsError::Decode(format!(
                "expected a Binary, LargeBinary or BinaryView array of serialized messages, got {}",
                other
            )))
            }
        };
        let batch = self.decode_binary_array(&binary)?;
        export_record_batch(batch)
    }

    /// Decode serialized protobuf messages into a record batch, returned as a
    /// struct-typed array over the C Data Interface.
    ///
    /// Convenience over [`Handler::decode`] for callers that do not already
    /// hold the messages in an Arrow array; `None` decodes to a null row.
    pub fn decode_bytes(
        &self,
        messages: &[Option<&[u8]>],
    ) -> Result<(ArrowArray, ArrowSchema), PtarsError> {
        let mut builder = BinaryBuilder::new();
        for message in messages {
            match message {
                Some(bytes) => builder.append_value(bytes),
                None => builder.append_null(),
            }
        }
        let batch = self.decode_binary_array(&builder.finish())?;
        export_record_batch(batch)
    }

    /// Encode a record batch (passed as a struct-typed array over the C Data
    /// Interface) into a Binary array of serialized protobuf messages,
    /// returned over the C Data Interface.
    pub fn encode(
        &self,
        array: ArrowArray,
        schema: &ArrowSchema,
    ) -> Result<(ArrowArray, ArrowSchema), PtarsError> {
        // SAFETY: the caller guarantees the structs describe valid Arrow data,
        // as required by the C Data Interface contract.
        let data = unsafe { from_ffi(import_array(array), import_schema_ref(schema)) }
            .map_err(|e| PtarsError::Arrow(e.to_string()))?;
        if !matches!(data.data_type(), DataType::Struct(_)) {
            return Err(PtarsError::Encode(format!(
                "expected a Struct array (a record batch), got {}",
                data.data_type()
            )));
        }
        let struct_array = StructArray::from(data);
        if struct_array.null_count() > 0 {
            return Err(PtarsError::Encode(
                "struct array with top-level nulls cannot be encoded as a record batch".to_string(),
            ));
        }
        let batch = RecordBatch::from(struct_array);
        let array_data = ptars_core::record_batch_to_array(&batch, &self.descriptor);
        let (out_array, out_schema) =
            to_ffi(&array_data).map_err(|e| PtarsError::Arrow(e.to_string()))?;
        Ok((export_array(out_array), export_schema(out_schema)))
    }

    fn decode_binary_array(&self, binary: &BinaryArray) -> Result<RecordBatch, PtarsError> {
        ptars_core::binary_array_to_record_batch_direct(binary, &self.descriptor, &self.config)
            .map_err(|e| PtarsError::Decode(e.to_string()))
    }
}

fn export_record_batch(batch: RecordBatch) -> Result<(ArrowArray, ArrowSchema), PtarsError> {
    let struct_array = StructArray::from(batch);
    let (out_array, out_schema) =
        to_ffi(&struct_array.to_data()).map_err(|e| PtarsError::Arrow(e.to_string()))?;
    Ok((export_array(out_array), export_schema(out_schema)))
}
