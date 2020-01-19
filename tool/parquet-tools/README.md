# parquet-tools
parquet-tools is a command line tool that aid in the inspection of parquet files.
More functions will be added.

## Build
cd parquet-tools && go build parquet-tools

## Description
### -cmd
schema/size/rowcount
### -file
parquet file name;
### -tag
print the go struct tags; default is false;
### -cat
cat records of parquet file.

## Example

### Output Schema

```bash
bash$ ./parquet-tools -cmd schema -file a.parquet -tag false
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
  "Tag": "name=parquet_go_root, repetitiontype=REQUIRED",
  "Fields": [
    {
      "Tag": "name=name, type=UTF8, repetitiontype=REQUIRED",
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
./parquet-tools -cmd cat -count 2 -file a.parquet 
```
