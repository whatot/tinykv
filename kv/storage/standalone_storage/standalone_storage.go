package standalone_storage

import (
	"bytes"

	"github.com/Connor1996/badger"
	"github.com/Connor1996/badger/y"
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
	options.ValueDir = conf.DBPath

	return &StandAloneStorage{
		options: options,
	}
}

func (s *StandAloneStorage) Start() error {
	if s.db != nil {
		return nil
	}

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
	err := s.Start()
	if err != nil {
		return nil, err
	}

	return &AloneStorageReader{s.db, 0}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	err := s.Start()
	if err != nil {
		return err
	}

	write_batch := engine_util.WriteBatch{}
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
	txn := r.inner.NewTransaction(false)
	iter := txn.NewIterator(badger.DefaultIteratorOptions)
	iter.Rewind()
	prefix := []byte(cf + "_")
	return &AloneIter{r, txn, iter, cf, &prefix}
}

func (r *AloneStorageReader) Close() {
	if r.iterCount > 0 {
		panic("Unclosed iterator")
	}
}

type AloneIter struct {
	reader    *AloneStorageReader
	txn       *badger.Txn
	inner     *badger.Iterator
	cf        string
	cf_prefix *[]byte
}

func (it *AloneIter) Item() engine_util.DBItem {
	item := it.inner.Item()
	if item != nil {
		new_key := bytes.TrimPrefix(item.Key(), *it.cf_prefix)
		return &AloneItem{new_key: &new_key, inner: item}
	} else {
		return nil
	}
}
func (it *AloneIter) Valid() bool {
	return it.inner.Valid()
}
func (it *AloneIter) Next() {
	if it.inner.Valid() {
		it.inner.Next()
		for it.inner.Valid() && !it.inner.ValidForPrefix(*it.cf_prefix) {
			it.inner.Next()
		}
	}
}
func (it *AloneIter) Seek(key []byte) {
	it.inner.Seek(engine_util.KeyWithCF(it.cf, key))
}
func (it *AloneIter) Close() {
	it.inner.Close()
	it.txn.Discard()
	it.reader.iterCount -= 1
}

type AloneItem struct {
	new_key *[]byte
	inner   *badger.Item
}

func (item *AloneItem) Key() []byte {
	return *item.new_key
}
func (item *AloneItem) KeyCopy(dst []byte) []byte {
	return y.SafeCopy(dst, *item.new_key)
}
func (item *AloneItem) Value() ([]byte, error) {
	return item.inner.Value()
}
func (item *AloneItem) ValueSize() int {
	return item.inner.ValueSize()
}
func (item *AloneItem) ValueCopy(dst []byte) ([]byte, error) {
	return item.inner.ValueCopy(dst)
}
