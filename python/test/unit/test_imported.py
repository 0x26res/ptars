from google.protobuf.descriptor import Descriptor
from ptars.internal import _get_dependencies
from typing import cast
from ptars import HandlerPool
from ptars_protos import bench_pb2, imported_pb2, importer_pb2, simple_pb2


def test_with_imported_proto():
    handler = HandlerPool([importer_pb2.DESCRIPTOR]).get_for_message(
        cast(Descriptor, importer_pb2.ImporterMessage.DESCRIPTOR)
    )
    handler.list_to_record_batch(
        [
            m.SerializeToString()
            for m in [
                importer_pb2.ImporterMessage(
                    imported_message=imported_pb2.ImportedMessage(
                        string_value="SYM1",
                        imported_enum=imported_pb2.ImportedEnum.CORE_ENUM_FOO,
                    ),
                )
            ]
        ]
    )


def test_get_dependencies():
    assert [d.name for d in _get_dependencies(importer_pb2.DESCRIPTOR)] == [
        "google/protobuf/timestamp.proto",
        "google/protobuf/wrappers.proto",
        "google/type/date.proto",
        "ptars_protos/imported.proto",
        "ptars_protos/importer.proto",
    ]

    assert [d.name for d in _get_dependencies(imported_pb2.DESCRIPTOR)] == [
        "google/protobuf/timestamp.proto",
        "google/protobuf/wrappers.proto",
        "google/type/date.proto",
        "ptars_protos/imported.proto",
    ]

    assert [d.name for d in _get_dependencies(simple_pb2.DESCRIPTOR)] == [
        "google/protobuf/timestamp.proto",
        "google/protobuf/wrappers.proto",
        "google/type/date.proto",
        "google/type/timeofday.proto",
        "ptars_protos/simple.proto",
    ]

    assert [d.name for d in _get_dependencies(bench_pb2.DESCRIPTOR)] == [
        "google/protobuf/empty.proto",
        "google/protobuf/timestamp.proto",
        "google/protobuf/wrappers.proto",
        "google/type/date.proto",
        "google/type/timeofday.proto",
        "ptars_protos/bench.proto",
    ]
