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
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	return &StandAloneStorage{
		db: engine_util.CreateDB("tinykv", conf),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

type badgerReader struct {
	db *badger.DB
}

func newBadgerReader(db *badger.DB) *badgerReader {
	reader := &badgerReader{
		db: db,
	}
	return reader
}

func (r *badgerReader) GetCF(cf string, key []byte) ([]byte, error) {
	res, err := engine_util.GetCF(r.db, cf, key)
	if err != nil {
		if err == badger.ErrKeyNotFound {
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}

func (r *badgerReader) IterCF(cf string) engine_util.DBIterator {
	txn := r.db.NewTransaction(false)
	return engine_util.NewCFIterator(cf, txn)
}

func (r *badgerReader) Close() {
	r.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return newBadgerReader(s.db), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		var err error
		switch m.Data.(type) {
		case storage.Put:
			err = engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
			break
		case storage.Delete:
			err = engine_util.DeleteCF(s.db, m.Cf(), m.Key())
			break
		default:
			err = errors.New("non-supported modify")
		}

		if err != nil {
			return err
		}
	}

	return nil
}
