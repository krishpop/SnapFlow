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
  bool Read(Key key, Version** result, uint64 txn_unique_id = 0, TableType tbl_type = CHECKING, const bool& val = 0);

  // Check whether apply or abort the write
  bool CheckWrite(Key key, Version* read_version, Txn* current_txn, TableType tbl_type = CHECKING);

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  void FinishWrite(Key key, Version* new_version, TableType tbl_type = CHECKING);

  // Init storage of multiple tables
  void InitStorage();

  // Init table
  unordered_map<Key, deque<Version*>*> InitTable();

  // Lock the version_list of key
  void Lock(Key key);

  // Unlock the version_list of key
  void Unlock(Key key);

  // Get the start timestamp for a version and transaction id
  uint64 GetBeginTimestamp(Version * v, uint64 my_id, Timestamp&, const bool& val = 0);

  // Get the end timestamp for a version and transaction id
  uint64 GetEndTimestamp(Version * v, uint64 my_id, Timestamp&, const bool& val = 0);

  // Put end timestamps into versions in storage
  void PutEndTimestamp(Version *, Version *, uint64);

  ~MVCCStorage();

 private:

  void SetTS(Timestamp & ts, int t, bool mode);

  void InitTS(Timestamp & ts);

  friend class TxnProcessor;

  // MVCC storage: vector of tables (maps with pointer to linked list of versions)
  // TO DO: pointers to maps or just the maps?
  vector<unordered_map<Key, deque<Version*>*>> mvcc_data_(2);
};

#endif  // _MVCC_STORAGE_H_
