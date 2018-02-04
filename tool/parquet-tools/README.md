# parquet-tools
parquet-tools is a command line tool that aid in the inspection of parquet files.

## Build
cd parquet-tools && go build parquet-tools

## Example

### Output Schema

```bash
bash$ parquet-tools -cmd=schema -file=a.parquet
bash$
----- Go struct -----
parquet_go_root struct{
  name string
  age int32
  id int64
  weight float32
  sex bool
  day int32
}
----- Json schema -----
{
  "Fields": [
    {
      "Tag": "name=name, type=UTF8, repetitiontype=REQUIRED"
    },
    {
      "Tag": "name=age, type=INT32, repetitiontype=REQUIRED"
    },
    {
      "Tag": "name=id, type=INT64, repetitiontype=REQUIRED"
    },
    {
      "Tag": "name=weight, type=FLOAT, repetitiontype=REQUIRED"
    },
    {
      "Tag": "name=sex, type=BOOLEAN, repetitiontype=REQUIRED"
    },
    {
      "Tag": "name=day, type=DATE, repetitiontype=REQUIRED"
    }
  ],
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED"
}

```