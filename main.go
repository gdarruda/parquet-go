package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"os"

	"github.com/apache/thrift/lib/go/thrift"
)

type Record struct {
	Key   []byte
	Value []byte
}

type PageType int8

const (
	DATA_PAGE PageType = iota
	INDEX_PAGE
)

type Statistics struct {
	nullCount int64
}

type DataPageHeader struct {
	numValues               int32
	encoding                int32
	definitionLevelEncoding int32
	repetitionLevelEncoding int32
}

type PageHeader struct {
	pageType             PageType
	uncompressedPageSize int32
	compressedPageSize   int32
	dataPageHeader       DataPageHeader
}

type ColumnMetadata struct {
	columnType            int32
	encodings             []int32
	pathInSchema          []string
	codec                 int32
	numValues             int64
	totalUncompressedSize int64
	totalCompressedSize   int64
	dataPageOffset        int64
	statistics            Statistics
}

type ColumnChunk struct {
	fileOffset int64
	metaData   ColumnMetadata
}

type RowGroup struct {
	columns       []ColumnChunk
	totalByteSize int64
	numRows       int64
}

type SchemaElement struct {
	colType     int32
	name        string
	numChildren int32
}

type FileMetaData struct {
	version    int32
	schema     []SchemaElement
	num_rows   int64
	row_groups []RowGroup
}

type Parquet struct {
	writer    thrift.TProtocol
	file      io.ReadWriter
	ctx       context.Context
	transport *thrift.StreamTransport
}

func (t *Parquet) finishStruct() {
	t.writer.WriteStructEnd(t.ctx)
	t.transport.Flush(t.ctx)
	t.file.Write([]byte{0})
}

func (t *Parquet) writeI32(name string, id int16, value int32) {

	t.writer.WriteFieldBegin(t.ctx, name, thrift.I32, id)
	t.writer.WriteI32(t.ctx, value)
	t.writer.WriteFieldEnd(t.ctx)
}

func (t *Parquet) writeI64(name string, id int16, value int64) {

	t.writer.WriteFieldBegin(t.ctx, name, thrift.I64, id)
	t.writer.WriteI64(t.ctx, value)
	t.writer.WriteFieldEnd(t.ctx)
}

func (t *Parquet) writeDataPageHeader(dataPageHeader DataPageHeader) {

	t.writer.WriteStructBegin(t.ctx, "DataPageHeader")

	t.writeI32("num_values", 1, dataPageHeader.numValues)
	t.writeI32("encoding", 2, dataPageHeader.encoding)
	t.writeI32("definition_level_encoding", 3, dataPageHeader.definitionLevelEncoding)
	t.writeI32("repetition_level_encoding", 4, dataPageHeader.repetitionLevelEncoding)

	t.finishStruct()
}

func (t *Parquet) writePageHeader(pageHeader PageHeader) {

	t.writer.WriteStructBegin(t.ctx, "PageHeader")

	t.writeI32("type", 1, int32(pageHeader.pageType))
	t.writeI32("uncompressed_page_size", 2, pageHeader.uncompressedPageSize)
	t.writeI32("compressed_page_size", 3, pageHeader.compressedPageSize)

	t.writer.WriteFieldBegin(t.ctx, "data_page_header", thrift.STRUCT, 5)
	t.writeDataPageHeader(pageHeader.dataPageHeader)
	t.writer.WriteFieldEnd(t.ctx)

	t.finishStruct()
}

func (t *Parquet) writeStatistics(statistics Statistics) {

	t.writer.WriteStructBegin(t.ctx, "Statistics")
	t.writeI64("null_count", 3, statistics.nullCount)
	t.finishStruct()

}

func (t *Parquet) writeColumnMetadata(metadata ColumnMetadata) {

	t.writer.WriteStructBegin(t.ctx, "ColumnMetadata")

	t.writeI32("type", 1, metadata.columnType)

	t.writer.WriteFieldBegin(t.ctx, "encodings", thrift.LIST, 2)
	t.writer.WriteListBegin(t.ctx, 4 /*int32*/, 1)
	for _, encoding := range metadata.encodings {
		t.writer.WriteI32(t.ctx, encoding)
	}
	t.writer.WriteListEnd(t.ctx)
	t.writer.WriteFieldEnd(t.ctx)

	t.writer.WriteFieldBegin(t.ctx, "path_in_schema", thrift.LIST, 3)
	t.writer.WriteListBegin(t.ctx, 11 /*string*/, 1)
	for _, path := range metadata.pathInSchema {
		t.writer.WriteBinary(t.ctx, []byte(path))
	}
	t.writer.WriteListEnd(t.ctx)
	t.writer.WriteFieldEnd(t.ctx)

	t.writeI32("codec", 4, metadata.codec)
	t.writeI64("num_values", 5, metadata.numValues)
	t.writeI64("total_uncompressed_size", 6, metadata.totalUncompressedSize)
	t.writeI64("total_compressed_size", 7, metadata.totalCompressedSize)
	t.writeI64("data_page_offset", 9, metadata.dataPageOffset)

	// writeStatistics
	t.writer.WriteFieldBegin(t.ctx, "statistics", thrift.STRUCT, 12)
	t.writeStatistics(metadata.statistics)
	t.writer.WriteFieldEnd(t.ctx)

	t.finishStruct()
}

func (t *Parquet) writeColumnChunk(columnChunk ColumnChunk) {

	t.writer.WriteStructBegin(t.ctx, "RowGroup")
	t.writeI64("file_offset", 2, columnChunk.fileOffset)

	t.writer.WriteFieldBegin(t.ctx, "meta_data", thrift.STRUCT, 3)
	t.writeColumnMetadata(columnChunk.metaData)
	t.finishStruct()
}

func (t *Parquet) writeRowGroup(rowGroup RowGroup) {

	t.writer.WriteStructBegin(t.ctx, "RowGroup")
	t.writer.WriteFieldBegin(t.ctx, "columns", thrift.LIST, 1)
	t.writer.WriteListBegin(t.ctx, thrift.STRUCT, len(rowGroup.columns))

	for _, col := range rowGroup.columns {
		t.writeColumnChunk(col)
	}
	t.writer.WriteListEnd(t.ctx)

	t.writeI64("total_byte_size", 2, rowGroup.totalByteSize)
	t.writeI64("total_byte_size", 3, rowGroup.numRows)

	t.finishStruct()
}

func (t *Parquet) writeSchema(schema SchemaElement) {

	t.writer.WriteStructBegin(t.ctx, "SchemaElement")
	t.writeI32("type", 1, schema.colType)
	t.writer.WriteFieldBegin(t.ctx, "name", 11 /*string*/, 4)
	t.writer.WriteBinary(t.ctx, []byte(schema.name))
	t.writer.WriteFieldEnd(t.ctx)
	t.writeI32("num_children", 5, schema.numChildren)
	t.finishStruct()

}

func (t *Parquet) writeFileMetadata(fileMetaData FileMetaData) {

	t.writer.WriteStructBegin(t.ctx, "FileMetaData")
	t.writeI32("version", 1, 1)

	t.writer.WriteFieldBegin(t.ctx, "schema", thrift.LIST, 2)
	t.writer.WriteListBegin(t.ctx, thrift.STRUCT, len(fileMetaData.schema))
	for _, col := range fileMetaData.schema {
		t.writeSchema(col)
	}
	t.writer.WriteListEnd(t.ctx)
	t.writer.WriteFieldEnd(t.ctx)

	t.writeI64("num_rows", 3, fileMetaData.num_rows)

	t.writer.WriteFieldBegin(t.ctx, "row_groups", thrift.LIST, 4)
	t.writer.WriteListBegin(t.ctx, thrift.STRUCT, len(fileMetaData.row_groups))

	for _, group := range fileMetaData.row_groups {
		t.writeRowGroup(group)
	}

	t.writer.WriteListEnd(t.ctx)
	t.writer.WriteFieldEnd(t.ctx)

	t.finishStruct()
}

func (t *Parquet) writeColumn(records [][]byte) {

	for _, record := range records {

		valueSize := uint32(len(record))

		size := make([]byte, 4)
		binary.LittleEndian.PutUint32(size, valueSize)
		t.file.Write(size)
		t.file.Write(record)
	}

	t.transport.Flush(t.ctx)

}

func TableParquet(buffer io.ReadWriter) Parquet {

	transport := thrift.NewStreamTransportRW(buffer)

	protocolFactory := thrift.NewTCompactProtocolFactoryConf(nil)
	protocol := protocolFactory.GetProtocol(transport)
	table := Parquet{
		writer:    protocol,
		file:      buffer,
		ctx:       context.TODO(),
		transport: transport}

	return table

}

func bufferToFile(buffer *bytes.Buffer, file *os.File) {

	for {

		b := buffer.Next(1)

		if len(b) == 0 {
			break
		}

		file.Write(b)

	}
}

func WritePage(column [][]byte, f *os.File) int {

	header := bytes.NewBuffer([]byte{})
	content := bytes.NewBuffer([]byte{})

	parquetHeader := TableParquet(header)
	defer parquetHeader.transport.Close()

	parquetContent := TableParquet(content)
	defer parquetContent.transport.Close()

	parquetContent.writeColumn(column)

	pageHeader := PageHeader{
		pageType:             DATA_PAGE,
		uncompressedPageSize: int32(content.Len()),
		compressedPageSize:   int32(content.Len()),
		dataPageHeader: DataPageHeader{
			numValues: int32(len(column)),
			encoding:  0 /* PLAIN */}}

	parquetHeader.writePageHeader(pageHeader)
	pageSize := content.Len() + header.Len()

	bufferToFile(header, f)
	bufferToFile(content, f)

	return pageSize
}

func magicBytes(f *os.File) {
	f.Write([]byte("PAR1"))
}

func main() {

	records := []Record{
		{Key: []byte("chave_1"), Value: []byte("valor_1")},
		{Key: []byte("chave_2"), Value: []byte("valor_2")},
		{Key: []byte("chave_3"), Value: []byte("valor_3")}}

	keys := make([][]byte, len(records))
	values := make([][]byte, len(records))

	for idx, record := range records {
		keys[idx] = record.Key
		values[idx] = record.Value
	}

	f, _ := os.Create("sample.parquet")
	defer f.Close()

	footer := bytes.NewBuffer([]byte{})
	parquetFooter := TableParquet(footer)
	defer parquetFooter.transport.Close()

	magicBytes(f)

	sizeKeys := WritePage(keys, f)
	sizeValues := WritePage(values, f)
	numRows := len(records)

	magicBytesOffset := 4

	schema := []SchemaElement{
		{name: "schema", numChildren: 2},
		{name: "key", colType: 6},
		{name: "values", colType: 6}}

	columns := []ColumnChunk{
		{fileOffset: int64(magicBytesOffset),
			metaData: ColumnMetadata{
				columnType:            6,
				encodings:             []int32{0},
				pathInSchema:          []string{"key"},
				codec:                 0,
				numValues:             int64(numRows),
				totalUncompressedSize: int64(sizeKeys),
				totalCompressedSize:   int64(sizeKeys),
				dataPageOffset:        int64(magicBytesOffset),
				statistics:            Statistics{nullCount: 0},
			},
		},
		{fileOffset: int64(magicBytesOffset + sizeKeys),
			metaData: ColumnMetadata{
				columnType:            6,
				encodings:             []int32{0},
				pathInSchema:          []string{"values"},
				codec:                 0,
				numValues:             int64(numRows),
				totalUncompressedSize: int64(sizeValues),
				totalCompressedSize:   int64(sizeValues),
				dataPageOffset:        int64(magicBytesOffset + sizeKeys),
				statistics:            Statistics{nullCount: 0},
			},
		},
	}

	parquetFooter.writeFileMetadata(
		FileMetaData{
			version:  1,
			schema:   schema,
			num_rows: int64(numRows),
			row_groups: []RowGroup{
				{columns: columns,
					totalByteSize: int64(sizeKeys + sizeValues),
					numRows:       int64(numRows)},
			},
		},
	)

	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(footer.Len()))
	parquetFooter.file.Write(bs)

	bufferToFile(footer, f)
	magicBytes(f)

}
