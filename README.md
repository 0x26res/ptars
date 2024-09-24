# ptars

[![PyPI Version][pypi-image]][pypi-url]
[![Python Version][versions-image]][versions-url]
[![Github Stars][stars-image]][stars-url]
[![codecov][codecov-image]][codecov-url]
[![Build Status][build-image]][build-url]
[![License][license-image]][license-url]
[![Downloads][downloads-image]][downloads-url]
[![Downloads][downloads-month-image]][downloads-month-url]
[![Code style: black][codestyle-image]][codestyle-url]
[![snyk][snyk-image]][snyk-url]

Protobuf to Arrow, using Rust

## Example

Take a protobuf:

```protobuf
message SearchRequest {
  string query = 1;
  int32 page_number = 2;
  int32 result_per_page = 3;
}
```

And convert serialized messages directly to `pyarrow.RecordBatch`:

```python
from ptars import HandlerPool


messages = [
    SearchRequest(
        query="protobuf to arrow",
        page_number=0,
        result_per_page=10,
    ),
    SearchRequest(
        query="protobuf to arrow",
        page_number=1,
        result_per_page=10,
    ),
]
payloads = [message.SerializeToString() for message in messages]

pool = HandlerPool()
handler = pool.get_for_message(SearchRequest.DESCRIPTOR)
record_batch = handler.list_to_record_batch(payloads)
```

| query             |   page_number |   result_per_page |
|:------------------|--------------:|------------------:|
| protobuf to arrow |             0 |                10 |
| protobuf to arrow |             1 |                10 |

You can also convert a `pyarrow.RecordBatch` back to serialized protobuf messages:

```python
array: pa.BinaryArray = handler.record_batch_to_array(record_batch)
messages_back: list[SearchRequest] = [
    SearchRequest.FromString(s.as_py()) for s in array
]
```

[pypi-image]: https://img.shields.io/pypi/v/ptars
[pypi-url]: https://pypi.org/project/ptars/
[build-image]: https://github.com/0x26res/ptars/actions/workflows/ci.yaml/badge.svg
[build-url]: https://github.com/0x26res/ptars/actions/workflows/ci.yaml
[stars-image]: https://img.shields.io/github/stars/0x26res/ptars
[stars-url]: https://github.com/0x26res/ptars
[versions-image]: https://img.shields.io/pypi/pyversions/ptars
[versions-url]: https://pypi.org/project/ptars/
[license-image]: http://img.shields.io/:license-Apache%202-blue.svg
[license-url]: https://github.com/0x26res/ptars/blob/master/LICENSE
[codecov-image]: https://codecov.io/gh/0x26res/ptars/branch/master/graph/badge.svg?token=XMFH27IL70
[codecov-url]: https://codecov.io/gh/0x26res/ptars
[downloads-image]: https://pepy.tech/badge/ptars
[downloads-url]: https://static.pepy.tech/badge/ptars
[downloads-month-image]: https://pepy.tech/badge/ptars/month
[downloads-month-url]: https://static.pepy.tech/badge/ptars/month
[codestyle-image]: https://img.shields.io/badge/code%20style-black-000000.svg
[codestyle-url]: https://github.com/ambv/black
[snyk-image]: https://snyk.io/advisor/python/ptars/badge.svg
[snyk-url]: https://snyk.io/advisor/python/ptars
