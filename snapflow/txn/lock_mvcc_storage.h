// Author: SNAPFLOW BOYS

#ifndef _LOCK_STORAGE_H_
#define _LOCK_STORAGE_H_

#include "txn/mvcc_storage.h"

using std::unordered_map;
using std::deque;
using std::map;
using std::vector;

// TODO: Should create a parent abstract Storage class
class LockMVCCStorage : public MVCCStorage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  bool Read(Key key, Version** result, uint64 txn_unique_id = 0, const TableType tbl_type = CHECKING, const bool& val = 0);

  // Check whether apply or abort the write
  bool LockCheckWrite(Key key, uint64 txn_unique_id, const TableType tbl_type = CHECKING);

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  // just a normal write in this case
  void FinishWrite(Key key, Version* new_version, const TableType tbl_type = CHECKING);

  // Init storage
  void InitStorage();

  // Init storage table
  unordered_map<Key, deque<Version*>*> InitTable(TableType tbl);

  // Lock the version_list of key
  void Lock(Key key, const TableType tbl_type);

  // Unlock the version_list of key
  void Unlock(Key key, const TableType tbl_type);

  virtual ~LockMVCCStorage();

 private:

  friend class TxnProcessor;

  // Storage for MVCC, each key has a linklist of versions
  vector<unordered_map<Key, deque<Version*>*>> lock_mvcc_data_;

  // Mutexs for each key
  vector<unordered_map<Key, Mutex*>> mutexs_;
};


#endif  // _LOCK_STORAGE_H_
