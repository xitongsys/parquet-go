# parquet-tools
parquet-tools is a command line tool that aid in the inspection of parquet files.
More functions will be added.

## Build
cd parquet-tools && go build parquet-tools

## Description
### -cmd
schema/size/rowcount/cat/metadata
### -count
max count to cat. If it is nil, only show first 1000 records. (default 1000)
### -file
parquet file name, supprt `file://` and `s3://` scheme
### -pretty
show pretty size
###  -schema-format
schema format, go/json. (default "json")
### -tag
print the go struct tags; default is false;
### -uncompressed
show uncompressed size

## Example

### Output Schema

```bash
bash$ ./parquet-tools -cmd schema -file a.parquet -tag false
{
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {
      "Tag": "name=name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=REQUIRED",
      "Fields": null
    },
    {
      "Tag": "name=age, type=INT32, repetitiontype=REQUIRED",
      "Fields": null
    },
    {
      "Tag": "name=id, type=INT64, repetitiontype=REQUIRED",
      "Fields": null
    },
    {
      "Tag": "name=weight, type=FLOAT, repetitiontype=REQUIRED",
      "Fields": null
    },
    {
      "Tag": "name=sex, type=BOOLEAN, repetitiontype=REQUIRED",
      "Fields": null
    },
    {
      "Tag": "name=day, type=DATE, repetitiontype=REQUIRED",
      "Fields": null
    }
  ]
}

```

### Show records
```bash
#show first 2 records of a.parquet
bash$ ./parquet-tools -cmd cat -count 2 -file a.parquet 
```

### Show metadata of an S3 object
```bash
# data is from https://pro.dp.la/developers/bulk-download
bash$ ./parquet-tools -cmd metadata -file s3://dpla-provider-export/2021/03/all.parquet/part-00000-56ba8f11-cb30-4bc3-8f6d-dd26c0dd1f06-c000.snappy.parquet
{
  "RowGroups": [
    {
      "Columns": [
        {
          "Name": "Doc.Uri",
          "ValueCount": 385959,
          "NullCount": 0,
          "UncompressedSize": 22774880,
          "CompressedSize": 12971526
        },
        {
          "Name": "Doc.Id",
          "ValueCount": 385959,
          "NullCount": 0,
          "UncompressedSize": 13895977,
          "CompressedSize": 12576719
        },
        {
          "Name": "Doc.DataProvider.List.Element",
          "ValueCount": 385959,
          "NullCount": 0,
          "UncompressedSize": 355531,
          "CompressedSize": 258478
        },
...
```
