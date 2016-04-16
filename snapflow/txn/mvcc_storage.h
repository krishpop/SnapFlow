// Author: Kun Ren (kun.ren@yale.edu)

#ifndef _MVCC_STORAGE_H_
#define _MVCC_STORAGE_H_

#include "txn/storage.h"
#include "limits.h"



// The upper limit for ints.
int INF_INT = std::numeric_limits<int>::max();

// MVCC storage
class MVCCStorage : public Storage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  virtual bool Read(Key key, Value* result, int txn_unique_id = 0);

  // Check whether apply or abort the write
  virtual bool CheckWrite(Key key, Version* read_version, txn* current_txn);

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  virtual void FinishWrite(Key key, Value value, txn* current_txn);

  // Returns the timestamp at which the record with the specified key was last
  // updated (returns 0 if the record has never been updated). This is used for OCC.
  virtual double Timestamp(Key key) {return 0;}

  // Init storage
  virtual void InitStorage();

  // Lock the version_list of key
  virtual void Lock(Key key);

  // Unlock the version_list of key
  virtual void Unlock(Key key);

  int GetBeginTimestamp(Version * v, int my_id, TxnTable * t);

  int GetEndTimestamp(Version * v, int my_id);

  void PutEndTimestamp(Version * old_version, Version * new_version);

  virtual ~MVCCStorage();

 private:

  void MVCCStorage::SetTS(SpeculativeTS & ts, int t, bool mode);

  void MVCCStorage::InitTS(SpeculativeTS & ts)

  friend class TxnProcessor;

  // Storage for MVCC, each key has a linklist of versions
  unordered_map<Key, deque<Version*>*> mvcc_data_;

  // Mutexs for each key
  unordered_map<Key, Atomic<int>> write_access_table_;


};

#endif  // _MVCC_STORAGE_H_
