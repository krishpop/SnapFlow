// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#ifndef _TXN_PROCESSOR_H_
#define _TXN_PROCESSOR_H_

#include <deque>
#include <map>
#include <string>

#include "txn/common.h"
#include "txn/storage.h"
#include "txn/txn.h"
#include "utils/atomic.h"
#include "utils/static_thread_pool.h"
#include "utils/mutex.h"
#include "utils/condition.h"


using std::deque;
using std::map;
using std::string;

enum CCMode {
  SERIAL = 0,
  SI = 1,                  // Snapshot isolation (by Larson)
  NEW = 2                // Our new control flow algo
};


class TxnProcessor {
 public:
  // The TxnProcessor's constructor starts the TxnProcessor running in the
  // background.
  explicit TxnProcessor(CCMode mode);

  // The TxnProcessor's destructor stops all background threads and deallocates
  // all objects currently owned by the TxnProcessor, except for Txn objects.
  ~TxnProcessor();

  // Registers a new txn request to be executed by the TxnProcessor.
  // Ownership of '*txn' is transfered to the TxnProcessor.
  void NewTxnRequest(Txn* txn);

  // Returns a pointer to the next COMMITTED or ABORTED Txn. The caller takes
  // ownership of the returned Txn.
  Txn* GetTxnResult();

  // Main loop implementing all concurrency control/thread scheduling.
  void RunScheduler();
  
  static void* StartScheduler(void * arg);
  // An instance transaction table of txns that have WRITTEN/TRIED TO WRITE
  // to the database
  
 private:

  // Performs all reads required to execute the transaction, then executes the
  // transaction logic.
  void ExecuteTxn(Txn* txn);

  // Serial version of scheduler.
  void RunSerialScheduler();

  // Applies all writes performed by '*txn' to 'storage_'.
  //
  // Requires: txn->Status() is COMPLETED_C.
  void ApplyWrites(Txn* txn);

  void GetBeginTimestamp(Txn* txn);

  void GetEndTimestamp(Txn* txn);

  void GetReads(Txn* txn);

  bool CheckWrites(Txn* txn);

  void FinishWrites(Txn* txn);

  void PutEndTimestamps(Txn* txn);

  void SnapshotExecuteTxn();

  // snapshot version of scheduler.
  void RunSnapshotScheduler();

  // run our new version
  void RunNewScheduler();
  
  void GarbageCollection();
  
  // Concurrency control mechanism the TxnProcessor is currently using.
  CCMode mode_;

  // Thread pool managing all threads used by TxnProcessor.
  StaticThreadPool tp_;

  // Data storage used for all modes.
  Storage* storage_;

  // Next valid unique_id, and a mutex to guard incoming txn requests.
  int next_unique_id_;
  Mutex mutex_;

  // Queue of incoming transaction requests.
  AtomicQueue<Txn*> txn_requests_;


  // THESE SEEM Deprecated, but I think we might want to implement
  // Queue of completed (but not yet committed/aborted) transactions.
  AtomicQueue<Txn*> completed_txns_;

  // Queue of transaction results (already committed or aborted) to be returned
  // to client.
  AtomicQueue<Txn*> txn_results_;



};

#endif  // _TXN_PROCESSOR_H_

