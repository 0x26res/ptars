use crate::{converter, CE_OFFSET};
use arrow::array::ArrayData;
use arrow::buffer::{Buffer, NullBuffer};
use arrow::datatypes::{ArrowNativeType, ToByteSlice};
use arrow_array::builder::Int32Builder;
use arrow_array::{
    Array, ArrayRef, ArrowPrimitiveType, BinaryArray, BooleanArray, Date32Array, Float32Array,
    Float64Array, Int32Array, Int64Array, ListArray, PrimitiveArray, RecordBatch, Scalar,
    StringArray, StructArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field};
use chrono::Datelike;
use prost::Message;
use prost_reflect::{DynamicMessage, FieldDescriptor, Kind, MessageDescriptor, Value};
use std::iter::zip;
use std::sync::Arc;

struct StringBuilder {
    values: String,
    offsets: Vec<i32>,
}

impl StringBuilder {
    pub fn new() -> Self {
        Self {
            values: String::new(),
            offsets: Vec::new(),
        }
    }

    fn append(&mut self, message: &DynamicMessage, field: &FieldDescriptor) {
        self.append_value(message.get_field(field).as_str().unwrap());
    }

    fn append_value(&mut self, value: &str) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        self.values.push_str(value);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    fn build(&mut self) -> Arc<dyn Array> {
        let size = self.offsets.len();
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());

        let array_data = ArrayData::builder(DataType::Utf8)
            .len(size)
            .add_buffer(Buffer::from_vec(self.offsets.to_vec()))
            .add_buffer(Buffer::from(self.values.as_bytes()))
            .build()
            .unwrap();
        Arc::new(StringArray::from(array_data))
    }
}

pub struct BinaryBuilder {
    values: Vec<u8>,
    offsets: Vec<i32>,
}

impl BinaryBuilder {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            offsets: Vec::new(),
        }
    }

    fn append(&mut self, message: &DynamicMessage, field: &FieldDescriptor) {
        self.append_bytes(message.get_field(field).as_bytes().unwrap());
    }

    fn append_bytes(&mut self, bytes: &prost::bytes::Bytes) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        for n in 0..bytes.len() {
            self.values.push(*bytes.get(n).unwrap());
        }
    }

    pub fn append_vec(&mut self, vec: &Vec<u8>) {
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());
        self.values.extend(vec);
    }

    fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn build(&mut self) -> Arc<dyn Array> {
        let size = self.offsets.len();
        self.offsets
            .push(i32::from_usize(self.values.len()).unwrap());

        // TODO: look into avoiding copy here
        let array_data = ArrayData::builder(DataType::Binary)
            .len(size)
            .add_buffer(Buffer::from(self.offsets.to_byte_slice()))
            .add_buffer(Buffer::from_iter(self.values.clone()))
            .build()
            .unwrap();
        Arc::new(BinaryArray::from(array_data))
    }
}

fn singular_field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &Vec<DynamicMessage>,
) -> Result<Arc<dyn Array>, &'static str> {
    match field_descriptor.kind() {
        Kind::Double => Ok(read_primitive::<f64, Float64Array>(
            messages,
            field_descriptor,
            &Value::as_f64,
            0.0,
        )),
        Kind::Float => Ok(read_primitive::<f32, Float32Array>(
            messages,
            field_descriptor,
            &Value::as_f32,
            0.0,
        )),
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => Ok(read_primitive::<i32, Int32Array>(
            messages,
            field_descriptor,
            &Value::as_i32,
            0,
        )),
        Kind::Sfixed64 | Kind::Sint64 | Kind::Int64 => Ok(read_primitive::<i64, Int64Array>(
            messages,
            field_descriptor,
            &Value::as_i64,
            0,
        )),
        Kind::Fixed32 | Kind::Uint32 => Ok(read_primitive::<u32, UInt32Array>(
            messages,
            field_descriptor,
            &Value::as_u32,
            0,
        )),
        Kind::Fixed64 | Kind::Uint64 => Ok(read_primitive::<u64, UInt64Array>(
            messages,
            field_descriptor,
            &Value::as_u64,
            0,
        )),
        Kind::Bool => Ok(read_primitive::<bool, BooleanArray>(
            messages,
            field_descriptor,
            &Value::as_bool,
            false,
        )),
        Kind::String => {
            let mut string_builder = StringBuilder::new();
            for message in messages {
                string_builder.append(message, field_descriptor)
            }
            Ok(string_builder.build())
        }
        Kind::Bytes => {
            let mut binary_builder = BinaryBuilder::new();
            for message in messages {
                binary_builder.append(message, field_descriptor)
            }
            Ok(binary_builder.build())
        }
        Kind::Message(x) => Ok(nested_messages_to_array(field_descriptor, &x, messages)),
        Kind::Enum(_) => Ok(read_primitive::<i32, Int32Array>(
            messages,
            field_descriptor,
            &Value::as_enum_number,
            0,
        )),
    }
}

fn read_i32(message: &DynamicMessage, field_descriptor: &FieldDescriptor) -> i32 {
    message.get_field(field_descriptor).as_i32().unwrap()
}

fn convert_date(
    messages: &Vec<DynamicMessage>,
    is_valid: &Vec<bool>,
    message_descriptor: &MessageDescriptor,
) -> Arc<Date32Array> {
    let year_descriptor = message_descriptor.get_field_by_name("year").unwrap();
    let month_descriptor = message_descriptor.get_field_by_name("month").unwrap();
    let day_descriptor = message_descriptor.get_field_by_name("day").unwrap();

    let mut builder = Int32Builder::new();
    for (message, message_valid) in zip(messages, is_valid) {
        if *message_valid {
            let year: i32 = read_i32(message, &year_descriptor);
            let month: i32 = read_i32(message, &month_descriptor);
            let day: i32 = read_i32(message, &day_descriptor);

            if (year == 0) && (month == 0) && (day == 0) {
                builder.append_value(0)
            } else {
                builder.append_value(
                    chrono::NaiveDate::from_ymd_opt(
                        year,
                        u32::try_from(month).unwrap(),
                        u32::try_from(day).unwrap(),
                    )
                    .unwrap()
                    .num_days_from_ce()
                        - CE_OFFSET,
                )
            }
        } else {
            builder.append_null()
        }
    }
    Arc::new(builder.finish().reinterpret_cast())
}

fn convert_timestamps(
    arrays: &[(Arc<Field>, Arc<dyn Array>)],
    is_valid: &[bool],
) -> Arc<TimestampNanosecondArray> {
    let scalar: Scalar<PrimitiveArray<arrow_array::types::Int64Type>> =
        Int64Array::new_scalar(1_000_000_000);
    let seconds: Arc<dyn Array> = arrays[0].clone().1;
    let nanos: Arc<dyn Array> = arrays[1].clone().1;
    let casted = arrow::compute::kernels::cast(&nanos, &DataType::Int64).unwrap();
    let multiplied = arrow::compute::kernels::numeric::mul(&seconds, &scalar).unwrap();
    let total: ArrayRef = arrow::compute::kernels::numeric::add(&multiplied, &casted).unwrap();

    let is_valid_array = BooleanArray::from(is_valid.to_owned());
    let is_null = arrow::compute::not(&is_valid_array).unwrap();
    let total_nullable = arrow::compute::nullif(&total, &is_null).unwrap();
    Arc::new(Int64Array::from(total_nullable.to_data()).reinterpret_cast())
}

fn nested_messages_to_array(
    field_descriptor: &FieldDescriptor,
    message_descriptor: &MessageDescriptor,
    messages: &Vec<DynamicMessage>,
) -> Arc<dyn Array> {
    let mut nested_messages: Vec<DynamicMessage> = Vec::new();
    let mut is_valid: Vec<bool> = Vec::new();
    for message in messages {
        let ee = message.get_field(field_descriptor);
        let each_value = ee.as_message().unwrap();
        nested_messages.push(each_value.clone());
        is_valid.push(message.has_field(field_descriptor));
    }
    if message_descriptor.full_name() == "google.type.Date" {
        convert_date(&nested_messages, &is_valid, message_descriptor)
    } else {
        let arrays: Vec<(Arc<Field>, Arc<dyn Array>)> =
            fields_to_arrays(&nested_messages, message_descriptor);
        if arrays.is_empty() {
            Arc::new(StructArray::new_empty_fields(
                nested_messages.len(),
                Some(NullBuffer::from_iter(is_valid)),
            ))
        } else if message_descriptor.full_name() == "google.protobuf.Timestamp" {
            convert_timestamps(&arrays, &is_valid)
        } else {
            Arc::new(StructArray::from((arrays, Buffer::from_iter(is_valid))))
        }
    }
}

fn read_primitive<'b, T: Clone, A: From<Vec<T>> + Array + 'static>(
    messages: &'b Vec<DynamicMessage>,
    field_descriptor: &FieldDescriptor,
    extractor: &dyn Fn(&Value) -> Option<T>,
    default: T,
) -> Arc<dyn Array> {
    let mut values: Vec<T> = Vec::new();
    for message in messages {
        if !field_descriptor.supports_presence() || message.has_field(field_descriptor) {
            values.push(extractor(&message.get_field(field_descriptor)).unwrap());
        } else {
            values.push(default.clone())
        }
    }
    Arc::new(A::from(values))
}

fn read_repeated_primitive<'b, T, A: From<Vec<T>> + Array>(
    field_descriptor: &FieldDescriptor,
    messages: &'b Vec<DynamicMessage>,
    data_type: DataType,
    extractor: &dyn Fn(&Value) -> Option<T>,
) -> Result<Arc<dyn Array>, &'static str> {
    let mut all_values: Vec<T> = Vec::new();
    let mut offsets: Vec<i32> = Vec::new();
    offsets.push(0);
    for message in messages {
        if message.has_field(field_descriptor) {
            let field_value = message.get_field(field_descriptor);
            println!("{}", field_descriptor.full_name());
            let field_value_as_list: &[Value] = field_value.as_list().unwrap();
            for each_value in field_value_as_list {
                all_values.push(extractor(each_value).unwrap())
            }
        }
        offsets.push(i32::from_usize(all_values.len()).unwrap());
    }
    let list_data_type = DataType::List(Arc::new(Field::new("item", data_type, false)));
    let list_data = ArrayData::builder(list_data_type)
        .len(messages.len())
        .add_buffer(Buffer::from_iter(offsets))
        .add_child_data(A::from(all_values).to_data())
        .build()
        .unwrap();
    Ok(Arc::new(ListArray::from(list_data)))
}

fn repeated_field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &Vec<DynamicMessage>,
) -> Result<Arc<dyn Array>, &'static str> {
    match field_descriptor.kind() {
        Kind::Double => read_repeated_primitive::<f64, Float64Array>(
            field_descriptor,
            messages,
            DataType::Float64,
            &Value::as_f64,
        ),
        Kind::Float => read_repeated_primitive::<f32, Float32Array>(
            field_descriptor,
            messages,
            DataType::Float32,
            &Value::as_f32,
        ),
        Kind::Sfixed32 | Kind::Sint32 | Kind::Int32 => read_repeated_primitive::<i32, Int32Array>(
            field_descriptor,
            messages,
            DataType::Int32,
            &Value::as_i32,
        ),
        Kind::Sfixed64 | Kind::Sint64 | Kind::Int64 => read_repeated_primitive::<i64, Int64Array>(
            field_descriptor,
            messages,
            DataType::Int64,
            &Value::as_i64,
        ),
        Kind::Fixed32 | Kind::Uint32 => read_repeated_primitive::<u32, UInt32Array>(
            field_descriptor,
            messages,
            DataType::UInt32,
            &Value::as_u32,
        ),
        Kind::Fixed64 | Kind::Uint64 => read_repeated_primitive::<u64, UInt64Array>(
            field_descriptor,
            messages,
            DataType::UInt64,
            &Value::as_u64,
        ),
        Kind::Bool => read_repeated_primitive::<bool, BooleanArray>(
            field_descriptor,
            messages,
            DataType::Boolean,
            &Value::as_bool,
        ),
        Kind::String => {
            let mut string_builder = StringBuilder::new();
            let mut offsets: Vec<i32> = Vec::new();
            offsets.push(0);
            for message in messages {
                if message.has_field(field_descriptor) {
                    let each_list = message.get_field(field_descriptor);
                    let values = each_list.as_list().unwrap();
                    for each_value in values {
                        string_builder.append_value(each_value.as_str().unwrap())
                    }
                }
                offsets.push(i32::from_usize(string_builder.len()).unwrap());
            }
            let list_data_type =
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false)));
            let list_data = ArrayData::builder(list_data_type)
                .len(messages.len())
                .add_buffer(Buffer::from_iter(offsets))
                .add_child_data(string_builder.build().to_data())
                .build()
                .unwrap();
            Ok(Arc::new(ListArray::from(list_data)))
        }
        Kind::Bytes => {
            let mut builder = BinaryBuilder::new();
            let mut offsets: Vec<i32> = Vec::new();
            offsets.push(0);
            for message in messages {
                if message.has_field(field_descriptor) {
                    let each_list = message.get_field(field_descriptor);
                    let values = each_list.as_list().unwrap();
                    for each_value in values {
                        builder.append_bytes(each_value.as_bytes().unwrap())
                    }
                }
                offsets.push(i32::from_usize(builder.len()).unwrap());
            }
            let list_data_type =
                DataType::List(Arc::new(Field::new("item", DataType::Binary, false)));
            let list_data = ArrayData::builder(list_data_type)
                .len(messages.len())
                .add_buffer(Buffer::from_iter(offsets))
                .add_child_data(builder.build().to_data())
                .build()
                .unwrap();
            Ok(Arc::new(ListArray::from(list_data)))
        }
        Kind::Message(_) => {
            let mut repeated_messages: Vec<DynamicMessage> = Vec::new();
            let mut offsets: Vec<i32> = Vec::new();
            offsets.push(0);
            for message in messages {
                for each_message in message.get_field(field_descriptor).as_list().unwrap() {
                    repeated_messages.push(each_message.as_message().unwrap().clone());
                }
                offsets.push(i32::from_usize(repeated_messages.len()).unwrap());
            }
            let arrays = fields_to_arrays(
                &repeated_messages,
                field_descriptor.kind().as_message().unwrap(),
            );
            let struct_array: Arc<StructArray> = if arrays.is_empty() {
                Arc::new(StructArray::new_empty_fields(repeated_messages.len(), None))
            } else {
                Arc::new(StructArray::from(arrays))
            };
            let list_data_type = DataType::List(Arc::new(Field::new(
                "item",
                struct_array.data_type().clone(),
                false,
            )));
            let list_data: ArrayData = ArrayData::builder(list_data_type)
                .len(messages.len())
                .add_buffer(Buffer::from_iter(offsets))
                .add_child_data(struct_array.to_data())
                .build()
                .unwrap();
            Ok(Arc::new(ListArray::from(list_data)))
        }
        Kind::Enum(_) => read_repeated_primitive::<i32, Int32Array>(
            field_descriptor,
            messages,
            DataType::Int32,
            &Value::as_enum_number,
        ),
    }
}

fn field_to_array(
    field_descriptor: &FieldDescriptor,
    messages: &Vec<DynamicMessage>,
) -> Result<Arc<dyn Array>, &'static str> {
    if field_descriptor.is_list() {
        repeated_field_to_array(field_descriptor, messages)
    } else if field_descriptor.is_map() {
        Err("map not supported")
    } else {
        singular_field_to_array(field_descriptor, messages)
    }
}

fn is_nullable(field: &FieldDescriptor) -> bool {
    field.supports_presence()
}

fn field_to_tuple(
    field: &FieldDescriptor,
    messages: &Vec<DynamicMessage>,
) -> Result<(Arc<Field>, Arc<dyn Array>), &'static str> {
    let results = field_to_array(field, messages);
    match results {
        Ok(array) => Ok((
            Arc::new(Field::new(
                field.name(),
                array.data_type().clone(),
                is_nullable(field),
            )),
            array,
        )),
        Err(x) => Err(x),
    }
}

pub fn fields_to_arrays(
    messages: &Vec<DynamicMessage>,
    message_descriptor: &MessageDescriptor,
) -> Vec<(Arc<Field>, Arc<dyn Array>)> {
    message_descriptor
        .fields()
        .map(|x| field_to_tuple(&x, messages).unwrap())
        .collect()
}

fn set_primitive<P: ArrowPrimitiveType>(
    array: &ArrayRef,
    messages: &mut [DynamicMessage],
    field_descriptor: &FieldDescriptor,
    value_creator: &dyn Fn(P::Native) -> Value,
) {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<P>>()
        .unwrap()
        .iter()
        .enumerate()
        .for_each(|(index, value)| match value {
            None => {}
            Some(x) => {
                let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                element.set_field(field_descriptor, value_creator(x));
            }
        })
}

pub fn extract_singular_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [DynamicMessage],
) {
    match field_descriptor.kind() {
        Kind::Int32 => {
            set_primitive::<arrow_array::types::Int32Type>(
                array,
                messages,
                field_descriptor,
                &Value::I32,
            );
        }
        Kind::Uint32 => {
            set_primitive::<arrow_array::types::UInt32Type>(
                array,
                messages,
                field_descriptor,
                &Value::U32,
            );
        }
        Kind::Int64 => set_primitive::<arrow_array::types::Int64Type>(
            array,
            messages,
            field_descriptor,
            &Value::I64,
        ),
        Kind::Uint64 => set_primitive::<arrow_array::types::UInt64Type>(
            array,
            messages,
            field_descriptor,
            &Value::U64,
        ),
        Kind::Float => set_primitive::<arrow_array::types::Float32Type>(
            array,
            messages,
            field_descriptor,
            &Value::F32,
        ),
        Kind::Double => set_primitive::<arrow_array::types::Float64Type>(
            array,
            messages,
            field_descriptor,
            &Value::F64,
        ),
        Kind::Bool => {
            // BooleanType doesn't implement primitive type
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .enumerate()
                .for_each(|(index, value)| match value {
                    None => {}
                    Some(x) => {
                        let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                        element.set_field(field_descriptor, Value::Bool(x));
                    }
                })
        }
        Kind::String => array
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut DynamicMessage = messages.get_mut(index).unwrap();
                    element.set_field(field_descriptor, Value::String(x.to_string()));
                }
            }),
        Kind::Bytes => array
            .as_any()
            .downcast_ref::<BinaryArray>()
            .unwrap()
            .iter()
            .enumerate()
            .for_each(|(index, value)| match value {
                None => {}
                Some(x) => {
                    let element: &mut DynamicMessage = messages.get_mut(index).unwrap();

                    element.set_field(
                        field_descriptor,
                        Value::Bytes(prost::bytes::Bytes::from(x.to_vec())),
                    );
                }
            }),
        Kind::Sint32 => {}
        Kind::Sint64 => {}
        Kind::Fixed32 => {}
        Kind::Fixed64 => {}
        Kind::Sfixed32 => {}
        Kind::Sfixed64 => {}
        Kind::Message(_) => {}
        Kind::Enum(_) => {}
    }
}

pub fn extract_array(
    array: &ArrayRef,
    field_descriptor: &FieldDescriptor,
    messages: &mut [DynamicMessage],
) {
    if field_descriptor.is_map() {
        // TODO:
    } else if field_descriptor.is_list() {
        // TODO
    } else {
        extract_singular_array(array, field_descriptor, messages)
    }
}

pub fn messages_to_record_batch(
    values: &[Vec<u8>],
    message_descriptor: &MessageDescriptor,
) -> RecordBatch {
    let messages: Vec<DynamicMessage> = values
        .iter()
        .map(|x| DynamicMessage::decode(message_descriptor.clone(), x.as_slice()).unwrap())
        .collect();
    let arrays: Vec<(Arc<Field>, Arc<dyn Array>)> =
        converter::fields_to_arrays(&messages, message_descriptor);
    let struct_array = if arrays.is_empty() {
        StructArray::new_empty_fields(messages.len(), None)
    } else {
        StructArray::from(arrays)
    };
    RecordBatch::from(struct_array)
}

pub fn record_batch_to_array(
    record_batch: &RecordBatch,
    message_descriptor: &MessageDescriptor,
) -> ArrayData {
    let mut messages: Vec<DynamicMessage> = (0..record_batch.num_rows())
        .map(|_| DynamicMessage::new(message_descriptor.clone()))
        .collect::<Vec<DynamicMessage>>();

    message_descriptor
        .fields()
        .for_each(|field_descriptor: FieldDescriptor| {
            let column: Option<&ArrayRef> = record_batch.column_by_name(field_descriptor.name());
            match column {
                None => {}
                Some(column) => converter::extract_array(column, &field_descriptor, &mut messages),
            }
        });
    let mut results = converter::BinaryBuilder::new();

    messages
        .iter()
        .for_each(|x| results.append_vec(&x.encode_to_vec()));
    return results.build().to_data();
}

#[cfg(test)]
mod tests {
    use super::*;

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
