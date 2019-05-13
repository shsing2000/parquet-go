package ParquetReader

import (
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xitongsys/parquet-go/mocks"
)

type example struct {
	ID    int64  `parquet:"name=id, type=INT64"`
	Value string `parquet:"name=value, type=UTF8"`
}

func TestNewParquetReader(t *testing.T) {
	t.Skip()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
		mockPF.EXPECT().Read(gomock.Any()),
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
