package ParquetReader

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xitongsys/parquet-go/mocks"
)

func TestSkipRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPF := mocks.NewMockParquetFile(ctrl)
	mockPF.EXPECT().Open(gomock.Any()).Return(mockPF, nil)
	cb, err := NewColumnBuffer(mockPF, nil, nil, "stuff")
	if err != nil {
		t.Errorf("expected error to be nil but got %s", err.Error())
	}
	cb.SkipRows(10)
}
