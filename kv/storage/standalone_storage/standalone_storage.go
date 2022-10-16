package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	kvDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	os.MkdirAll(kvPath, os.ModePerm)
	kvDB := engine_util.CreateDB(kvPath, false)
	store := &StandAloneStorage{
		kvDB: kvDB,
	}
	return store
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	s.kvDB.Close()
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.kvDB.NewTransaction(true)
	return &standaloneReader{s, txn, 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			err := engine_util.PutCF(s.kvDB, put.Cf, put.Key, put.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.kvDB, delete.Cf, delete.Key)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type standaloneReader struct {
	inner     *StandAloneStorage
	txn       *badger.Txn
	iterCount int
}

func (r *standaloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *standaloneReader) IterCF(cf string) engine_util.DBIterator {
	iter := engine_util.NewCFIterator(cf, r.txn)
	return &standaloneIter{iter: iter}
}

func (r *standaloneReader) Close() {
	r.txn.Discard()
}

type standaloneIter struct {
	iter *engine_util.BadgerIterator
}

func (i *standaloneIter) Item() engine_util.DBItem {
	return i.iter.Item()
}

func (i *standaloneIter) Valid() bool {
	if !i.iter.Valid() {
		return false
	}
	return true
}

func (i *standaloneIter) Next() {
	i.iter.Next()
}

func (i *standaloneIter) Seek(key []byte) {
	i.iter.Seek(key)
}

func (i *standaloneIter) Close() {
	i.iter.Close()
}
