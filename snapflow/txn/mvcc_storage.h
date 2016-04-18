// Author: Kun Ren (kun.ren@yale.edu)

#ifndef _MVCC_STORAGE_H_
#define _MVCC_STORAGE_H_

#include <limits.h>
#include <tr1/unordered_map>
#include <deque>
#include <map>

#include "txn/common.h"
#include "txn/txn.h"
#include "utils/mutex.h"

using std::tr1::unordered_map;
using std::deque;
using std::map;


// MVCC storage
class MVCCStorage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  bool Read(Key key, Version* result, uint64 txn_unique_id = 0);

  // Check whether apply or abort the write
  bool CheckWrite(Key key, Version* read_version, Txn* current_txn);

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  void FinishWrite(Key key, Version* new_version);

  // Init storage
  void InitStorage();

  // Lock the version_list of key
  void Lock(Key key);

  // Unlock the version_list of key
  void Unlock(Key key);

  uint64 GetBeginTimestamp(Version * v, uint64 my_id, Timestamp&);

  uint64 GetEndTimestamp(Version * v, uint64 my_id, Timestamp&);

  void PutEndTimestamp(Version * old_version, Version * new_version);

  ~MVCCStorage();

 private:

  void SetTS(Timestamp & ts, int t, bool mode);

  void InitTS(Timestamp & ts);

  friend class TxnProcessor;

  // Storage for MVCC, each key has a linklist of versions
  unordered_map<Key, deque<Version*>*> mvcc_data_;

  // Mutexs for each key
  //unordered_map<Key, Atomic<int>> write_access_table_;


};

#endif  // _MVCC_STORAGE_H_
