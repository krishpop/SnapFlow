// Author: Kun Ren (kun.ren@yale.edu)

#ifndef _MVCC_STORAGE_H_
#define _MVCC_STORAGE_H_

#include <limits.h>
#include <unordered_map>
#include <deque>
#include <map>

#include "txn/common.h"
#include "txn/txn.h"
#include "utils/mutex.h"

using std::unordered_map;
using std::deque;
using std::map;
using std::vector;


// MVCC storage
class MVCCStorage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  virtual bool Read(Key key, Version** result, uint64 txn_unique_id = 0, TableType tbl_type = CHECKING, const bool& val = 0);

  // Check whether apply or abort the write
  bool CheckWrite(Key key, Version* read_version, Txn* current_txn, TableType tbl_type = CHECKING);

  // Check whether apply or abort the write
  virtual bool LockCheckWrite(Key key, uint64 txn_unique_id, TableType tbl_type = CHECKING) { return true; }

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  virtual void FinishWrite(Key key, Version* new_version, TableType tbl_type = CHECKING);

  // Init storage of multiple tables
  virtual void InitStorage();

  // Init table
  virtual unordered_map<Key, deque<Version*>*> InitTable(TableType tbl);

  // Lock the version_list of key
  virtual void Lock(Key key, TableType tbl_type){};

  // Unlock the version_list of key
  virtual void Unlock(Key key, TableType tbl_type){};

  // Get the start timestamp for a version and transaction id
  uint64 GetBeginTimestamp(Version * v, uint64 my_id, Timestamp&, const bool& val = 0);

  // Get the end timestamp for a version and transaction id
  uint64 GetEndTimestamp(Version * v, uint64 my_id, Timestamp&, const bool& val = 0);

  // Put end timestamps into versions in storage
  void PutEndTimestamp(Version *, Version *, uint64);

  virtual ~MVCCStorage();

 private:

  void SetTS(Timestamp & ts, int t, bool mode);

  void InitTS(Timestamp & ts);

  friend class TxnProcessor;

  // MVCC storage: vector of tables (maps with pointer to linked list of versions)
  // TO DO: pointers to maps or just the maps?
  vector<unordered_map<Key, deque<Version*>*>> mvcc_data_;
};

#endif  // _MVCC_STORAGE_H_
