package ParquetReader

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/golang/mock/gomock"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/mocks"
	"github.com/xitongsys/parquet-go/parquet"
)

type example struct {
	ID    int64  `parquet:"name=id, type=INT64"`
	Value string `parquet:"name=value, type=UTF8"`
}

func TestNewParquetReader(t *testing.T) {
	t.Skip()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	schemaHandler, err := SchemaHandler.NewSchemaHandlerFromStruct(new(example))
	if err != nil {
		t.Errorf("expected schema error to be nil but got %s", err.Error())
	}

	footer := &parquet.FileMetaData{
		Version: 1,
		Schema:  schemaHandler.SchemaElements,
		NumRows: 1,
		RowGroups: []*parquet.RowGroup{
			&parquet.RowGroup{
				Columns:       []*parquet.ColumnChunk{},
				TotalByteSize: 10,
				NumRows:       1,
			},
		},
		KeyValueMetadata: nil,
		CreatedBy:        nil,
	}

	for i, elem := range schemaHandler.SchemaElements {
		if elem.GetNumChildren() == 0 {
			path := schemaHandler.IndexMap[int32(i)]
			footer.RowGroups[0].Columns = append(footer.RowGroups[0].Columns, &parquet.ColumnChunk{
				FilePath:   &path,
				FileOffset: int64(i),
			})
		}
	}

	mockPF := mocks.NewMockParquetFile(ctrl)
	gomock.InOrder(
		mockPF.EXPECT().Seek(int64(-8), io.SeekEnd).Return(int64(0), nil),
		mockPF.EXPECT().Read(gomock.Any()).DoAndReturn(func(buf []byte) (int, error) {
			if len(buf) != 4 {
				t.Errorf("expected footer size slice to have length 4 but got %d", len(buf))
			}

			binary.LittleEndian.PutUint32(buf, 32)
			return 0, nil
		}),
		mockPF.EXPECT().Seek(int64(-40), io.SeekEnd).Return(int64(0), nil),
		mockPF.EXPECT().Read(gomock.Any()).DoAndReturn(func(buf []byte) (int, error) {
			ts := thrift.NewTSerializer()
			ts.Protocol = thrift.NewTCompactProtocolFactory().GetProtocol(ts.Transport)
			footerBuf, err := ts.Write(context.TODO(), footer)
			if err != nil {
				t.Error("expected footer thrift serialization error to be nil")
			}
			copy(buf, footerBuf)
			return len(footerBuf), io.EOF
		}),
		mockPF.EXPECT().Open(gomock.Any()).Return(mockPF, nil),
	)

	reader, err := NewParquetReader(mockPF, new(example), 1)
	if err != nil {
		t.Errorf("expected error to be nil but got %s", err.Error())
	}

	if reader == nil {
		t.Errorf("expected reader to be %T but got nil", reader)
	}
}

func TestNewParquetReaderSeekFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMessage := "some seek failure"
	mockPF := mocks.NewMockParquetFile(ctrl)
	mockPF.EXPECT().Seek(int64(-8), io.SeekEnd).Return(int64(0), errors.New(errMessage))

	reader, err := NewParquetReader(mockPF, new(example), 1)
	if err.Error() != errMessage {
		t.Errorf("expected error to be %q but got %q", errMessage, err.Error())
	}

	if reader != nil {
		t.Errorf("expected reader to be nil but got %T", reader)
	}
}

func TestNewParquetReaderMetadataError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	errMessage := "some read error"
	mockPF := mocks.NewMockParquetFile(ctrl)
	gomock.InOrder(
		mockPF.EXPECT().Seek(int64(-8), io.SeekEnd).Return(int64(0), nil),
		mockPF.EXPECT().Read(gomock.Any()).DoAndReturn(func(buf []byte) (int, error) {
			if len(buf) != 4 {
				t.Errorf("expected footer size slice to have length 4 but got %d", len(buf))
			}

			binary.LittleEndian.PutUint32(buf, 32)
			return 0, nil
		}),
		mockPF.EXPECT().Seek(int64(-40), io.SeekEnd).Return(int64(0), nil),
		mockPF.EXPECT().Read(gomock.Any()).Return(0, errors.New(errMessage)),
	)

	reader, err := NewParquetReader(mockPF, new(example), 1)
	if !strings.Contains(err.Error(), errMessage) {
		t.Errorf("expected error to be %q but got %q", errMessage, err.Error())
	}

	if reader != nil {
		t.Errorf("expected reader to be nil but got %T", reader)
	}
}
