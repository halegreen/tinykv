package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	var getErr error
	val, getErr := reader.GetCF(req.Cf, req.Key)
	notFound := false
	if err == nil && val == nil {
		notFound = true
	}
	resp.Value = val
	resp.NotFound = notFound
	if getErr != nil {
		resp.Error = getErr.Error()
	}
	return resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Put{
				Cf:    req.Cf,
				Key:   req.Key,
				Value: req.Value,
			},
		},
	}
	err := server.storage.Write(&kvrpcpb.Context{}, batch)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	batch := []storage.Modify{
		{
			Data: storage.Delete{
				Cf:  req.Cf,
				Key: req.Key,
			},
		},
	}
	err := server.storage.Write(&kvrpcpb.Context{}, batch)
	if err != nil {
		resp.Error = err.Error()
	}
	return resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	iter := reader.IterCF(req.Cf)
	defer iter.Close()

	kvs := []*kvrpcpb.KvPair{}
	limit := uint32(0)
	for iter.Seek(req.StartKey); iter.Valid() && limit < req.Limit; iter.Next() {
		item := iter.Item()
		val, err := item.Value()
		if err != nil {
			resp.Error = err.Error()
			return resp, err
		}
		kv := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: val,
		}
		kvs = append(kvs, kv)
		limit++
	}

	resp.Kvs = kvs
	return resp, nil
}
