package ParquetReader

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xitongsys/parquet-go/ParquetFile"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/parquet"
)

func TestSkipRowsAfterFailedRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMessage := "some read error"
	mockPF := ParquetFile.NewMockParquetFile(ctrl)
	mockPF.EXPECT().Seek(gomock.Any(), gomock.Any()).Return(int64(0), nil)
	mockPF.EXPECT().Read(gomock.Any()).Return(0, errors.New(errMessage))

	schemaHandler, err := SchemaHandler.NewSchemaHandlerFromStruct(new(example))
	if err != nil {
		t.Errorf("expected schema error to be nil but got %s", err.Error())
	}

	cb := &ColumnBufferType{
		PFile:            mockPF,
		SchemaHandler:    schemaHandler,
		DataTableNumRows: 5,
		ChunkHeader: &parquet.ColumnChunk{
			MetaData: &parquet.ColumnMetaData{
				NumValues: 10,
			},
		},
		ThriftReader: ParquetFile.ConvertToThriftReader(mockPF, 0, 10),
	}

	rowsToSkip := int64(10)
	num := cb.SkipRows(rowsToSkip)
	if num != 0 {
		t.Errorf("expected skip rows to return %d but got %d", 0, num)
	}
}
