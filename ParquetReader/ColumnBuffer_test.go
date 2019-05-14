package ParquetReader

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/xitongsys/parquet-go/SchemaHandler"
	"github.com/xitongsys/parquet-go/mocks"
	"github.com/xitongsys/parquet-go/parquet"
)

func TestNewColumnBuffer(t *testing.T) {
	t.Skip()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPF := mocks.NewMockParquetFile(ctrl)
	mockPF.EXPECT().Open(gomock.Any()).Return(mockPF, nil)

	schemaHandler, err := SchemaHandler.NewSchemaHandlerFromStruct(new(example))
	if err != nil {
		t.Errorf("expected schema error to be nil but got %s", err.Error())
	}

	var path string
	for i, elem := range schemaHandler.SchemaElements {
		if elem.Name == "Value" {
			path = schemaHandler.IndexMap[int32(i)]
		}
	}
	if len(path) == 0 {
		t.Error(`expected to find a path for schema element "Value"`)
	}

	fileMetadata := &parquet.FileMetaData{
		Version:          1,
		Schema:           schemaHandler.SchemaElements,
		NumRows:          1,
		RowGroups:        nil,
		KeyValueMetadata: nil,
		CreatedBy:        nil,
	}
	cb, err := NewColumnBuffer(mockPF, fileMetadata, schemaHandler, path)
	if err != nil {
		t.Errorf("expected error to be nil but got %s", err.Error())
	}
	if cb == nil {
		t.Errorf("expected column buffer to be nil but got %T", cb)
	}
}

func TestSkipRows(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockPF := mocks.NewMockParquetFile(ctrl)

	schemaHandler, err := SchemaHandler.NewSchemaHandlerFromStruct(new(example))
	if err != nil {
		t.Errorf("expected schema error to be nil but got %s", err.Error())
	}
	// spew.Dump(schemaHandler)
	var path string
	for i, elem := range schemaHandler.SchemaElements {
		if elem.Name == "Value" {
			path = schemaHandler.IndexMap[int32(i)]
		}
	}
	if len(path) == 0 {
		t.Error(`expected to find a path for schema element "Value"`)
	}

	fileMetadata := &parquet.FileMetaData{
		Version:          1,
		Schema:           schemaHandler.SchemaElements,
		NumRows:          1,
		RowGroups:        nil,
		KeyValueMetadata: nil,
		CreatedBy:        nil,
	}
	cb := &ColumnBufferType{
		PFile:            mockPF,
		Footer:           fileMetadata,
		SchemaHandler:    schemaHandler,
		PathStr:          path,
		DataTableNumRows: -1,
	}

	rowsToSkip := int64(10)
	num := cb.SkipRows(rowsToSkip)
	if num != 0 {
		t.Errorf("expected skip rows to return 0 but got %d", num)
	}
}
