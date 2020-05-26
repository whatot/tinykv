package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	options badger.Options
	db      *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	options := badger.DefaultOptions
	options.Dir = conf.DBPath

	return &StandAloneStorage{
		options: options,
	}
}

func (s *StandAloneStorage) Start() error {
	db, err := badger.Open(s.options)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	if s.db == nil {
		return nil, errors.New("db haven't started yet")
	}

	return &AloneStorageReader{s.db, 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	write_batch := new(engine_util.WriteBatch)
	defer write_batch.Reset()

	for _, m := range batch {
		switch data := m.Data.(type) {
		case storage.Put:
			write_batch.SetCF(data.Cf, data.Key, data.Value)
		case storage.Delete:
			write_batch.DeleteCF(data.Cf, data.Key)
		}
	}

	return write_batch.WriteToDB(s.db)
}

type AloneStorageReader struct {
	inner     *badger.DB
	iterCount int
}

func (r *AloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	var res []byte
	err := r.inner.View(func(txn *badger.Txn) error {
		item, err1 := txn.Get(engine_util.KeyWithCF(cf, key))
		if err1 != nil {
			return err1
		}
		v, err2 := item.Value()
		if err2 != nil {
			return err2
		}
		res = v
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (r *AloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	// TODO impl iter
	return nil
}

func (r *AloneStorageReader) Close() {
	if r.iterCount > 0 {
		panic("Unclosed iterator")
	}
}
