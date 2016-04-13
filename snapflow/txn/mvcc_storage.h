// Author: Kun Ren (kun.ren@yale.edu)

#ifndef _MVCC_STORAGE_H_
#define _MVCC_STORAGE_H_

#include "txn/storage.h"
#include "limits.h"


struct TimeStamp {
  int timestamp;
  Txn* txn;
  bool edit_bit;
};

// MVCC 'version' structure
struct Version {
  Value value_;      // The value of this version
  TimeStamp begin_id_; // The timestamp of the earliest possible transaction to read/write this version
  TimeStamp begin_id_; // Timestamp of the latest possible transaction to read/write this version
};

// The upper limit for ints.
int INF_INT = std::numeric_limits<int>::max();

// MVCC storage
class MVCCStorage : public Storage {
 public:
  // If there exists a record for the specified key, sets '*result' equal to
  // the value associated with the key and returns true, else returns false;
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  virtual bool Read(Key key, Value* result, int txn_unique_id = 0);

  // Inserts a new version with key and value
  // The third parameter is the txn_unique_id(txn timestamp), which is used for MVCC.
  virtual void Write(Key key, Value value, int txn_unique_id = 0);

  // Returns the timestamp at which the record with the specified key was last
  // updated (returns 0 if the record has never been updated). This is used for OCC.
  virtual double Timestamp(Key key) {return 0;}

  // Init storage
  virtual void InitStorage();

  // Lock the version_list of key
  virtual void Lock(Key key);

  // Unlock the version_list of key
  virtual void Unlock(Key key);

  // Check whether apply or abort the write
  virtual bool CheckWrite (Key key, int txn_unique_id);

  int GetBeginTimestamp(Version * v, int my_id, TxnTable * t);

  int MVCCStorage::GetEndTimestamp(Version * v, int my_id);

  virtual ~MVCCStorage();

 private:

  void MVCCStorage::SetTs(SpeculativeTS & ts, int t, bool mode);

  void MVCCStorage::InitTS(SpeculativeTS & ts)

  friend class TxnProcessor;

  // Storage for MVCC, each key has a linklist of versions
  unordered_map<Key, deque<Version*>*> mvcc_data_;

  // Mutexs for each key
  unordered_map<Key, Mutex*> mutexs_;

};

#endif  // _MVCC_STORAGE_H_
