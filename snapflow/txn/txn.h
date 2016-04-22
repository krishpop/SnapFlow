// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#ifndef _TXN_H_
#define _TXN_H_

#include <map>
#include <set>
#include <vector>

#include "txn/common.h"
#include "utils/atomic.h"

using std::map;
using std::set;
using std::vector;
// The upper limit for ints.
extern uint64 INF_INT;
// Txns can have five distinct status values:
enum TxnStatus {
  INCOMPLETE = 0,   // Not yet executed
  ACTIVE = 1,
  COMPLETED_C = 2,
  COMPLETED_A = 3,
  COMMITTED = 4,    // Committed
  ABORTED = 5      // Aborted
  // TODO add a PREPARING state?
};

struct Timestamp {
  uint64 timestamp;
  void* txn;
  Atomic<int> edit_bit;
  Mutex mutex_; // Mutex for setting txn in version end_id_ to txn that is overwriting
};

// MVCC 'version' structure
struct Version {
  Value value_;      // The value of this version
  Timestamp begin_id_; // The timestamp of the earliest possible transaction to read/write this version
  Timestamp end_id_; // Timestamp of the latest possible transaction to read/write this version
};

class Txn {
 public:

  Txn() : status_(INCOMPLETE) {}
  virtual ~Txn() {}
  virtual Txn * clone() const = 0;    // Virtual constructor (copying)

  // Method containing all the transaction's method logic.
  virtual void Run() = 0;

  // For txns not of type WriteCheck, this just returns true.
  // For WriteCheck txns, this evaluates whether the original constraint path
  // is the same at commit time. Called by TxnProcessor.
  virtual bool Validate() { return true; }

  // Checks for overlap in read and write sets. If any key appears in both,
  // an error occurs.
  void CheckReadWriteSets();

  // Returns the Txn's current execution status.
  TxnStatus Status() { return status_; }

  uint64 GetStartID() { return unique_id_; }

  uint64 GetEndID() { return end_unique_id_; }

 protected:
  // Copies the internals of this txn into a given transaction (i.e.
  // the readset, writeset, and so forth).  Be sure to modify this method
  // to copy any new data structures you create.
  void CopyTxnInternals(Txn* txn) const;

  friend class TxnProcessor;

  // Method to be used inside 'Execute()' function when reading records from
  // the database. If record corresponding with specified 'key' exists, sets
  // '*value' equal to the record value and returns true, else returns false.
  //
  // Requires: key appears in readset or writeset
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  bool Read(const Key& key, Value* value);

  // Method to be used inside 'Execute()' function when writing records to
  // the database.
  //
  // Requires: key appears in writeset
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  void Write(const Key& key, const Value& value, Version*);

  // Macro to be used inside 'Execute()' function when deciding to COMMIT.
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  #define COMMIT \
    do { \
      status_ = COMMITTED; \
      return; \
    } while (0)

  // Macro to be used inside 'Execute()' function when deciding to ABORT.
  //
  // Note: Can ONLY be called from inside the 'Execute()' function.
  #define ABORT \
    do { \
      status_ = ABORTED; \
      return; \
    } while (0)

  // Set of all keys that may need to be read in order to execute the
  // transaction.
  set<Key> readset_;

  // Set of all keys that may be updated when executing the transaction.
  set<Key> writeset_;

  // Results of reads performed by the transaction.
  map<Key, Version*> reads_;

  // Key, Value pairs WRITTEN by the transaction.
  map<Key, Version*> writes_;

  // Transaction's current execution status.
  TxnStatus status_;

  // Unique, monotonically increasing transaction ID, assigned by TxnProcessor.
  uint64 unique_id_;

  // Unique, monotonically increasing transaction ID, assigned by TxnProcessor.
  uint64 end_unique_id_;

};



#endif  // _TXN_H_
