# Parquet V2 compression fixtures

These synthetic files contain nine rows in three row groups, a nullable UTF-8
column named `$.Name`, and an INT64 column. The column codec is SNAPPY or ZSTD,
but individual V2 pages can store raw values with `is_compressed=false`.
The reader must honor the page flag when decoding the values section.

Generated and independently read back with PyArrow 25.0.1. To regenerate from
this directory:

```python
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

values = [
    8029869417314771535, 7459148140755457688, 8038307628875046077,
    -3304027516717407273, 2641360246305232568, -7317965330839581364,
    -2967672239572933942, 8793262333459754672, 2612882932671687636,
]
table = pa.table({
    '$.Name': pa.array(
        [None if i % 3 == 1 else 'repeated name' for i in range(9)],
        pa.string(),
    ),
    'Value': pa.array(values, pa.int64()),
})
for codec in ('snappy', 'zstd'):
    path = Path(f'data-page-v2-{codec}.parquet')
    pq.write_table(
        table, path, compression=codec, data_page_version='2.0',
        use_dictionary=False, row_group_size=4, store_schema=False,
    )
    assert pq.read_table(path).equals(table)
```
