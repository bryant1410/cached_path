# `cached_path`

Originally copied from
[allenai/allennlp/common/file_utils
(commit f639336)](https://github.com/allenai/allennlp/blob/f639336/allennlp/common/file_utils.py),
then adapted.

## Differences with allenai/allennlp version

* Detect archive URLs automatically.
* Sync S3 prefixes (not just objects).
* Support RAR files.
* Caches the S3 and requests sessions (make it scale better when calling the function multiple times).
* More typing.
* Enhanced timeout.
* Different cache folder location.

## TODO

* [ ] Merge with the latest from allenai/allennlp.
* [ ] Add tests, including the ones under allenai/allennlp.
* [ ] Make AWS dependencies optional.
* [ ] An offline mode (no HEAD request).
