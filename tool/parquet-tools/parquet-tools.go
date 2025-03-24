package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/hangxie/parquet-go-source/local"
	"github.com/hangxie/parquet-go-source/s3"
	"github.com/hangxie/parquet-go/reader"
	"github.com/hangxie/parquet-go/source"
	"github.com/hangxie/parquet-go/tool/parquet-tools/schematool"
	"github.com/hangxie/parquet-go/tool/parquet-tools/sizetool"
)

func main() {
	cmd := flag.String("cmd", "schema", "command to run. Allowed values: schema, rowcount, size, cat")
	fileName := flag.String("file", "", "file name")
	withTags := flag.Bool("tag", false, "show struct tags")
	withPrettySize := flag.Bool("pretty", false, "show pretty size")
	uncompressedSize := flag.Bool("uncompressed", false, "show uncompressed size")
	catCount := flag.Int("count", 1000, "max count to cat. If it is nil, only show first 1000 records.")
	skipCount := flag.Int64("skip", 0, "skip count with cat. If it is nil,skip 0 records.")
	schemaFormat := flag.String("schema-format", "json", "schema format go/json (default to JSON schema)")

	flag.Parse()

	// validate schema output format
	if *schemaFormat != "json" && *schemaFormat != "go" {
		fmt.Fprintf(os.Stderr, "schema format can only be json or go\n")
		os.Exit(1)
	}

	// validate file name
	if *fileName == "" {
		fmt.Fprintf(os.Stderr, "missing location of parquet file\n")
		os.Exit(1)
	}

	// validate file scheme (s3 or file)
	uri, err := url.Parse(*fileName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unable to parse file location [%s]\n", *fileName)
		os.Exit(1)
	}
	if uri.Scheme == "" {
		uri.Scheme = "file"
	}

	var fr source.ParquetFile
	switch uri.Scheme {
	case "s3":
		// determine S3 bucket's region
		ctx := context.Background()
		sess := session.Must(session.NewSession())
		region, err := s3manager.GetBucketRegion(ctx, sess, uri.Host, "us-east-1")
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
				fmt.Fprintf(os.Stderr, "unable to find bucket %s's region not found", uri.Host)
			} else {
				fmt.Fprintf(os.Stderr, "AWS error: %s", err.Error())
			}
			os.Exit(1)
		}

		fr, err = s3.NewS3FileReader(ctx, uri.Host, strings.TrimLeft(uri.Path, "/"), &aws.Config{Region: aws.String(region)})
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open S3 object [%s]: %s\n", *fileName, err.Error())
			os.Exit(1)
		}
	case "file":
		fr, err = local.NewLocalFileReader(uri.Path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open local file [%s]: %s\n", uri.Path, err.Error())
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown location scheme [%s]\n", uri.Scheme)
		os.Exit(1)
	}

	pr, err := reader.NewParquetReader(fr, nil, 1)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Can't create parquet reader: %s\n", err)
		os.Exit(1)
	}

	switch *cmd {
	case "schema":
		tree := schematool.CreateSchemaTree(pr.SchemaHandler.SchemaElements)
		if *schemaFormat == "go" {
			fmt.Printf("%s\n", tree.OutputStruct(*withTags))
		} else {
			fmt.Printf("%s\n", tree.OutputJsonSchema())
		}
	case "rowcount":
		fmt.Println(pr.GetNumRows())
	case "size":
		fmt.Println(sizetool.GetParquetFileSize(*fileName, pr, *withPrettySize, *uncompressedSize))
	case "cat":
		totCnt := 0
		for totCnt < *catCount {
			cnt := *catCount - totCnt
			if cnt > 1000 {
				cnt = 1000
			}

			err = pr.SkipRows(*skipCount)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can't skip[: %s\n", err)
				os.Exit(1)
			}

			res, err := pr.ReadByNumber(cnt)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can't cat: %s\n", err)
				os.Exit(1)
			}

			jsonBs, err := json.Marshal(res)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Can't to json: %s\n", err)
				os.Exit(1)
			}

			fmt.Println(string(jsonBs))

			totCnt += cnt
		}

	default:
		fmt.Fprintf(os.Stderr, "Unknown command %s\n", *cmd)
		os.Exit(1)
	}
}
