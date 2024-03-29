// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)


#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {

  if (mode_ == MVCC) {
    storage_ = new LockMVCCStorage();
  }
  else {
    storage_ = new MVCCStorage();
  }

  storage_->InitStorage();
  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));

}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  txn_requests_.Push(txn);
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SI:                 RunSnapshotScheduler();
    case CSI:                RunCSIScheduler();
    case MVCC:               RunMVCCScheduler();
  }
}

//////////////////////// NORMAL MVCC /////////////////////////////////////////

bool TxnProcessor::MVCCCheckWrites(Txn* txn) {
    //   Call MVCCStorage::CheckWrite method to check all keys in the write_set_
  for (set<Key>::iterator it = txn->writeset_[CHECKING].begin();
       it != txn->writeset_[CHECKING].end(); ++it) {
    if (!storage_->LockCheckWrite(*it, txn->unique_id_, CHECKING)) {
      return false;
    }
  }

  for (set<Key>::iterator it = txn->writeset_[SAVINGS].begin();
       it != txn->writeset_[SAVINGS].end(); ++it) {
    if (!storage_->LockCheckWrite(*it, txn->unique_id_, SAVINGS)) {
      return false;
    }
  }
  return true;
}

void TxnProcessor::MVCCLockWriteKeys(Txn* txn) {
  //   Acquire all locks for keys in the write_set_
  for (set<Key>::iterator it = txn->writeset_[CHECKING].begin();
             it != txn->writeset_[CHECKING].end(); ++it) {
    storage_->Lock(*it, CHECKING);
  }
  for (set<Key>::iterator it = txn->writeset_[SAVINGS].begin();
             it != txn->writeset_[SAVINGS].end(); ++it) {
    storage_->Lock(*it, SAVINGS);
  }
}

void TxnProcessor::MVCCUnlockWriteKeys(Txn* txn) {
    //   Acquire all locks for keys in the write_set_
  for (set<Key>::iterator it = txn->writeset_[CHECKING].begin();
             it != txn->writeset_[CHECKING].end(); ++it) {
    storage_->Unlock(*it, CHECKING);
  }
  for (set<Key>::iterator it = txn->writeset_[SAVINGS].begin();
             it != txn->writeset_[SAVINGS].end(); ++it) {
    storage_->Unlock(*it, SAVINGS);
  }
}

void TxnProcessor::MVCCPerformReads(Txn* txn) {
  for (set<Key>::iterator it = txn->readset_[CHECKING].begin();
       it != txn->readset_[CHECKING].end(); ++it) {

    storage_->Lock(*it, CHECKING);
    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, CHECKING))
      txn->reads_[CHECKING][*it] = result;
    storage_->Unlock(*it, CHECKING);
  }

  for (set<Key>::iterator it = txn->readset_[SAVINGS].begin();
       it != txn->readset_[SAVINGS].end(); ++it) {

    storage_->Lock(*it, SAVINGS);
    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, SAVINGS))
      txn->reads_[SAVINGS][*it] = result;
    storage_->Unlock(*it, SAVINGS);
  }

  for (set<Key>::iterator it = txn->writeset_[CHECKING].begin();
       it != txn->writeset_[CHECKING].end(); ++it) {

    storage_->Lock(*it, CHECKING);
    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, CHECKING))
      txn->reads_[CHECKING][*it] = result;
    storage_->Unlock(*it, CHECKING);
  }

  for (set<Key>::iterator it = txn->writeset_[SAVINGS].begin();
       it != txn->writeset_[SAVINGS].end(); ++it) {

    storage_->Lock(*it, SAVINGS);
    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, SAVINGS))
      txn->reads_[SAVINGS][*it] = result;
    storage_->Unlock(*it, SAVINGS);
  }
}

void TxnProcessor::MVCCFinishWrites(Txn* txn) {
  for (map<Key, Version*>::iterator it = txn->writes_[CHECKING].begin();
       it != txn->writes_[CHECKING].end(); ++it) {
    storage_->FinishWrite(it->first, it->second, CHECKING);
  }

  for (map<Key, Version*>::iterator it = txn->writes_[SAVINGS].begin();
       it != txn->writes_[SAVINGS].end(); ++it) {
    storage_->FinishWrite(it->first, it->second, SAVINGS);
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn* txn) {

  GetBeginTimestamp(txn);

  //   Read all necessary data for this transaction from storage (Note that you should lock the key before each read)
  MVCCPerformReads(txn);

  //   Execute the transaction logic (i.e. call Run() on the transaction)
  txn->Run();

  // get all write locks
  MVCCLockWriteKeys(txn);

  bool good_writes = MVCCCheckWrites(txn);

  if (good_writes) {
    MVCCFinishWrites(txn);
    MVCCUnlockWriteKeys(txn);

    // Mark txn as committed
    txn->status_ = COMMITTED;
    txn_results_.Push(txn);

  } else {
    MVCCUnlockWriteKeys(txn);

    EmptyReadWrites(txn);
    txn->status_ = ABORTED;
    Txn* copy = txn->clone();
    copy->status_ = INCOMPLETE;
    txn_requests_.Push(copy);

  }


}

void TxnProcessor::RunMVCCScheduler() {
  Txn* txn;

  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::MVCCExecuteTxn,
            txn));
    }
  }
}

/////////////////////////// END OF NORMAL MVCC ///////////////////////////////

/////////////////////// START OF SI AND CSI EXECUTION ///////////////////////////

void TxnProcessor::GetBeginTimestamp(Txn* txn) {

  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  // This might be a race condition from CheckWrite in mvcc_storage when checking ABORTED
  txn->status_ = ACTIVE;
  next_unique_id_++;
  mutex_.Unlock();
}

void TxnProcessor::GetEndTimestamp(Txn* txn, const bool& val) {

  mutex_.Lock();
  txn->end_unique_id_ = next_unique_id_;
  if (!val) {
    txn->status_ = COMMITTED;
  }
  next_unique_id_++;
  mutex_.Unlock();
}

bool TxnProcessor::GetReads(Txn* txn) {

  for (set<Key>::iterator it = txn->readset_[CHECKING].begin();
     it != txn->readset_[CHECKING].end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, CHECKING)) {
      txn->reads_[CHECKING][*it] = result;
    }
    else {
      return false;
    }
  }

  for (set<Key>::iterator it = txn->readset_[SAVINGS].begin();
     it != txn->readset_[SAVINGS].end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, SAVINGS)) {
      txn->reads_[SAVINGS][*it] = result;
    }
    else {
      return false;
    }
  }
  return true;
}

void TxnProcessor::GetValidationReads(Txn* txn) {
  for (set<Key>::iterator it = txn->constraintset_.begin();
     it != txn->constraintset_.end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->end_unique_id_, CHECKING, true)) {
      txn->vals_[CHECKING][*it] = result;
    }
    result = NULL;
    if (storage_->Read(*it, &result, txn->end_unique_id_, SAVINGS, true)) {
      txn->vals_[SAVINGS][*it] = result;
    }

  }
}

bool TxnProcessor::CheckWrites(Txn* txn) {

  for (set<Key>::iterator it = txn->writeset_[CHECKING].begin();
     it != txn->writeset_[CHECKING].end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, CHECKING)) {
      txn->reads_[CHECKING][*it] = result;

      if (!storage_->CheckWrite(*it, result, txn, CHECKING)) {
        return false;
      }
    }
    else {
      // std::cout << "Did not find valid version for key: " << *it << std::endl;
      // storage_->Read(*it, &result, txn->unique_id_, CHECKING);
      return false;
    }
  }

  for (set<Key>::iterator it = txn->writeset_[SAVINGS].begin();
     it != txn->writeset_[SAVINGS].end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_, SAVINGS)) {
      txn->reads_[SAVINGS][*it] = result;

      if (!storage_->CheckWrite(*it, result, txn, SAVINGS)) {
        return false;
      }
    }
    else {
      return false;
    }
  }
  return true;

}

void TxnProcessor::FinishWrites(Txn* txn) {

  for (map<Key, Version*>::iterator it = txn->writes_[CHECKING].begin();
     it != txn->writes_[CHECKING].end(); ++it) {

    // first is pointer to version, 2nd is txn
    storage_->FinishWrite(it->first, it->second, CHECKING);

  }

  for (map<Key, Version*>::iterator it = txn->writes_[SAVINGS].begin();
     it != txn->writes_[SAVINGS].end(); ++it) {

    // first is pointer to version, 2nd is txn
    storage_->FinishWrite(it->first, it->second, SAVINGS);

  }

}

void TxnProcessor::PutEndTimestamps(Txn* txn) {

  for (map<Key, Version*>::iterator it = txn->writes_[CHECKING].begin();
     it != txn->writes_[CHECKING].end(); ++it) {

    if (txn->reads_[CHECKING][it->first] == NULL) {
      std::cout << "HERE" << std::endl;
    }
    // first is the old version, 2nd is new version
    storage_->PutEndTimestamp(txn->reads_[CHECKING][it->first], txn->writes_[CHECKING][it->first], txn->end_unique_id_);

  }

  for (map<Key, Version*>::iterator it = txn->writes_[SAVINGS].begin();
     it != txn->writes_[SAVINGS].end(); ++it) {

    // first is the old version, 2nd is new version
    storage_->PutEndTimestamp(txn->reads_[SAVINGS][it->first], txn->writes_[SAVINGS][it->first], txn->end_unique_id_);

  }

}

void TxnProcessor::EmptyReadWrites(Txn* txn) {
  txn->reads_[CHECKING].clear();
  txn->writes_[CHECKING].clear();
  txn->vals_[CHECKING].clear();
  txn->reads_[SAVINGS].clear();
  txn->writes_[SAVINGS].clear();
  txn->vals_[SAVINGS].clear();

}

void TxnProcessor::CSIExecuteTxn(Txn* txn) {
  // Begin stage
  GetBeginTimestamp(txn);

  // Normal execution stage
  // For all transactions that reach end of version deque
  // and do not get valid version to read, abort it
  // OR if
  if (!GetReads(txn) || !CheckWrites(txn)) {
    EmptyReadWrites(txn);
    Txn* copy = txn->clone();
    copy->status_ = INCOMPLETE;
    txn->status_ = ABORTED;

    // Copy txn
    txn_requests_.Push(copy);
    return;
  }


  txn->Run();
  // txn->writes_[CHECKING].empty();
  // txn->writes_[SAVINGS].empty();

  // If it's aborted here, it is a permanent abort
  if (txn->Status() == ABORTED) {
    EmptyReadWrites(txn);
    txn_results_.Push(txn);
    return;
  }

  FinishWrites(txn);
  GetEndTimestamp(txn);
  GetValidationReads(txn);

  if (txn->Validate()) {
    txn->status_ = COMMITTED;
  }
  else {
    EmptyReadWrites(txn);
    Txn* copy = txn->clone();
    copy->status_ = INCOMPLETE;
    txn->status_ = ABORTED;

    // Copy txn
    txn_requests_.Push(copy);
    return;
  }

  // Postprocessing Phase
  if (txn->Status() == COMMITTED){
    PutEndTimestamps(txn);
    txn_results_.Push(txn);
  }
}

void TxnProcessor::SnapshotExecuteTxn(Txn* txn) {

  GetBeginTimestamp(txn);

  if (!GetReads(txn) || !CheckWrites(txn)) {
    EmptyReadWrites(txn);
    Txn* copy = txn->clone();
    copy->status_ = INCOMPLETE;
    txn->status_ = ABORTED;
    // Copy txn
    txn_requests_.Push(copy);
    return;
  }

  if (txn->Status() == ACTIVE) {
    txn->Run();
    if (txn->Status() != ABORTED) {
      FinishWrites(txn);
      bool val = false;
      GetEndTimestamp(txn, val);
    }
    // If it's aborted here, it is a permanent abort
    else {

      EmptyReadWrites(txn);
      txn_results_.Push(txn);

    }
  }

  if (txn->Status() == COMMITTED){
    PutEndTimestamps(txn);
    txn_results_.Push(txn);
  }
}

void TxnProcessor::RunSnapshotScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::SnapshotExecuteTxn,
            txn));
    }
  }
}

void TxnProcessor::RunCSIScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::CSIExecuteTxn,
            txn));
    }
  }
}

/////////////////////// END OF SI AND CSI EXECUTION /////////////////////////////
