package reader_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func TestReadDataPageV2Compression(t *testing.T) {
	type row struct {
		Name  *string `parquet:"name=$.Name, type=BYTE_ARRAY, convertedtype=UTF8, repetitiontype=OPTIONAL"`
		Value *int64  `parquet:"name=Value, type=INT64, repetitiontype=OPTIONAL"`
	}
	name := "repeated name"
	values := []int64{
		8029869417314771535, 7459148140755457688, 8038307628875046077,
		-3304027516717407273, 2641360246305232568, -7317965330839581364,
		-2967672239572933942, 8793262333459754672, 2612882932671687636,
	}
	want := make([]row, len(values))
	for i := range values {
		want[i].Value = &values[i]
		if i%3 != 1 {
			want[i].Name = &name
		}
	}

	for _, codec := range []string{"snappy", "zstd"} {
		for _, parallelism := range []int64{1, 5} {
			t.Run(fmt.Sprintf("%s/parallelism=%d", codec, parallelism), func(t *testing.T) {
				file, err := local.NewLocalFileReader("testdata/data-page-v2-" + codec + ".parquet")
				require.NoError(t, err)
				defer file.Close()

				pr, err := reader.NewParquetReader(file, new(row), parallelism)
				require.NoError(t, err)
				defer pr.ReadStop()
				require.Equal(t, int64(len(want)), pr.GetNumRows())

				var got []row
				for _, count := range []int{2, 4, 3} {
					batch := make([]row, count)
					require.NoError(t, pr.Read(&batch))
					require.Len(t, batch, count)
					got = append(got, batch...)
				}
				require.Equal(t, want, got)
			})
		}
	}
}
