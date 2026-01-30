# ptars

[![PyPI Version][pypi-image]][pypi-url]
[![Python Version][versions-image]][versions-url]
[![PyPI Wheel][wheel-image]][wheel-url]
[![Documentation][docs-image]][docs-url]
[![Downloads][downloads-image]][downloads-url]
[![Downloads][downloads-month-image]][downloads-month-url]
[![Crates.io][crates-image]][crates-url]
[![Crates.io Downloads][crates-downloads-image]][crates-url]
[![docs.rs][docsrs-image]][docsrs-url]
[![Build Status][build-image]][build-url]
[![codecov][codecov-image]][codecov-url]
[![License][license-image]][license-url]
[![Ruff][ruff-image]][ruff-url]
[![snyk][snyk-image]][snyk-url]
[![Github Stars][stars-image]][stars-url]
[![GitHub issues][github-issues-image]][github-issues-url]
[![GitHub Release][release-image]][release-url]
[![Release Date][release-date-image]][release-url]
[![Last Commit][last-commit-image]][commits-url]
[![Commit Activity][commit-activity-image]][commits-url]
[![Open PRs][open-prs-image]][prs-url]
[![Contributors][contributors-image]][contributors-url]
[![Contributing][contributing-image]][contributing-url]
[![FOSSA Status][fossa-image]][fossa-url]
[![Repo Size][repo-size-image]][repo-size-url]

[Repository](https://github.com/0x26res/ptars) |
[Python Documentation](https://ptars.readthedocs.io/) |
[Python Installation](https://ptars.readthedocs.io/en/latest/getting-started/#installation) |
[PyPI](https://pypi.org/project/ptars/) |
[Rust Crate](https://crates.io/crates/ptars) |
[Rust Documentation](https://docs.rs/ptars)

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

pool = HandlerPool([SearchRequest.DESCRIPTOR.file])
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

## Configuration

Customize Arrow type mappings with `PtarsConfig`:

```python
from ptars import HandlerPool, PtarsConfig

config = PtarsConfig(
    timestamp_unit="us",  # microseconds instead of nanoseconds
    timestamp_tz="America/New_York",
)

pool = HandlerPool([SearchRequest.DESCRIPTOR.file], config=config)
```

## Benchmark against protarrow

[Ptars](https://github.com/0x26res/ptars) is a rust implementation of
[protarrow](https://github.com/tradewelltech/protarrow),
which is implemented in plain python.
It is:

- 2.5 times faster when converting from proto to arrow.
- 3 times faster when converting from arrow to proto.

```benchmark
---- benchmark 'to_arrow': 2 tests ----
Name (time in ms)        Mean          
---------------------------------------
protarrow_to_arrow     9.4863 (2.63)   
ptars_to_arrow         3.6009 (1.0)    
---------------------------------------

---- benchmark 'to_proto': 2 tests -----
Name (time in ms)         Mean          
----------------------------------------
protarrow_to_proto     20.8297 (3.20)   
ptars_to_proto          6.5013 (1.0)    
----------------------------------------
```

[pypi-image]: https://img.shields.io/pypi/v/ptars
[pypi-url]: https://pypi.org/project/ptars/
[versions-image]: https://img.shields.io/pypi/pyversions/ptars
[versions-url]: https://pypi.org/project/ptars/
[wheel-image]: https://img.shields.io/pypi/wheel/ptars
[wheel-url]: https://pypi.org/project/ptars/
[docs-image]: https://readthedocs.org/projects/ptars/badge/?version=latest
[docs-url]: https://ptars.readthedocs.io/en/latest/
[downloads-image]: https://pepy.tech/badge/ptars
[downloads-url]: https://static.pepy.tech/badge/ptars
[downloads-month-image]: https://pepy.tech/badge/ptars/month
[downloads-month-url]: https://static.pepy.tech/badge/ptars/month
[build-image]: https://github.com/0x26res/ptars/actions/workflows/ci.yaml/badge.svg
[build-url]: https://github.com/0x26res/ptars/actions/workflows/ci.yaml
[codecov-image]: https://codecov.io/gh/0x26res/ptars/branch/main/graph/badge.svg?token=XMFH27IL70
[codecov-url]: https://codecov.io/gh/0x26res/ptars
[license-image]: http://img.shields.io/:license-Apache%202-blue.svg
[license-url]: https://github.com/0x26res/ptars/blob/master/LICENSE
[ruff-image]: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json
[ruff-url]: https://github.com/astral-sh/ruff
[snyk-image]: https://snyk.io/advisor/python/ptars/badge.svg
[snyk-url]: https://snyk.io/advisor/python/ptars
[stars-image]: https://img.shields.io/github/stars/0x26res/ptars
[stars-url]: https://github.com/0x26res/ptars
[github-issues-image]: https://img.shields.io/badge/issue_tracking-github-blue.svg
[github-issues-url]: https://github.com/0x26res/ptars/issues
[contributing-image]: https://img.shields.io/badge/PR-Welcome-%23FF8300.svg?
[contributing-url]: https://github.com/0x26res/ptars/blob/main/DEVELOPMENT.md
[fossa-image]: https://app.fossa.com/api/projects/git%2Bgithub.com%2F0x26res%2Fptars.svg?type=shield
[fossa-url]: https://app.fossa.com/projects/git%2Bgithub.com%2F0x26res%2Fptars?ref=badge_shield
[repo-size-image]: https://img.shields.io/github/repo-size/0x26res/ptars
[repo-size-url]: https://img.shields.io/github/repo-size/0x26res/ptars
[crates-image]: https://img.shields.io/crates/v/ptars
[crates-url]: https://crates.io/crates/ptars
[crates-downloads-image]: https://img.shields.io/crates/d/ptars
[docsrs-image]: https://docs.rs/ptars/badge.svg
[docsrs-url]: https://docs.rs/ptars
[last-commit-image]: https://img.shields.io/github/last-commit/0x26res/ptars
[commits-url]: https://github.com/0x26res/ptars/commits/main
[commit-activity-image]: https://img.shields.io/github/commit-activity/m/0x26res/ptars
[open-prs-image]: https://img.shields.io/github/issues-pr/0x26res/ptars
[prs-url]: https://github.com/0x26res/ptars/pulls
[contributors-image]: https://img.shields.io/github/contributors/0x26res/ptars
[contributors-url]: https://github.com/0x26res/ptars/graphs/contributors
[release-image]: https://img.shields.io/github/v/release/0x26res/ptars
[release-date-image]: https://img.shields.io/github/release-date/0x26res/ptars
[release-url]: https://github.com/0x26res/ptars/releases
