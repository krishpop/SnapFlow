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
  
  

  storage_ = new MVCCStorage();
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
    case SI:                 RunSnapshotScheduler(); break;
    case NEW:                RunNewScheduler();
  }
}

// void TxnProcessor::RunSerialScheduler() {
//   Txn* txn;
//   while (tp_.Active()) {
//     // Get next txn request.
//     if (txn_requests_.Pop(&txn)) {
//       // Execute txn.
//       ExecuteTxn(txn);

//       // Commit/abort txn according to program logic's commit/abort decision.
//       if (txn->Status() == COMPLETED_C) {
//         //ApplyWrites(txn);
//         txn->status_ = COMMITTED;
//       } else if (txn->Status() == COMPLETED_A) {
//         txn->status_ = ABORTED;
//       } else {
//         // Invalid TxnStatus!
//         DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
//       }

//       // Return result to client.
//       txn_results_.Push(txn);
//     }
//   }
// }


// void TxnProcessor::ExecuteTxn(Txn* txn) {

//   // Read everything in from readset.
//   for (set<Key>::iterator it = txn->readset_.begin();
//        it != txn->readset_.end(); ++it) {
//     // Save each read result iff record exists in storage.
//     Version* result;
//     if (storage_->Read(*it, result))
//       txn->reads_[*it] = result;
//   }

//   // Also read everything in from writeset.
//   for (set<Key>::iterator it = txn->writeset_.begin();
//        it != txn->writeset_.end(); ++it) {
//     // Save each read result iff record exists in storage.
//     Version* result;
//     if (storage_->Read(*it, result))
//       txn->reads_[*it] = result;
//   }

//   // Execute txn's program logic.
//   txn->Run();

//   // Hand the txn back to the RunScheduler thread.
//   completed_txns_.Push(txn);
// }

// // Need to delete
// void TxnProcessor::ApplyWrites(Txn* txn) {
//   // Write buffered writes out to storage.
//   for (map<Key, Version*>::iterator it = txn->writes_.begin();
//        it != txn->writes_.end(); ++it) {
//     //storage_->Write(it->first, it->second, txn->unique_id_);
//   }
// }

/////////////////////// START OF SNAPSHOT EXECUTION ///////////////////////////

void TxnProcessor::GetBeginTimestamp(Txn* txn) {

  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  txn->status_ = ACTIVE;
  next_unique_id_++;
  mutex_.Unlock();
}

void TxnProcessor::GetEndTimestamp(Txn* txn) {

  mutex_.Lock();
  txn->end_unique_id_ = next_unique_id_;
  txn->status_ = COMMITTED;
  next_unique_id_++;
  mutex_.Unlock();
}

void TxnProcessor::GetReads(Txn* txn) {

  for (set<Key>::iterator it = txn->readset_.begin();
     it != txn->readset_.end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->reads_[*it] = result;
    }
  }

}

bool TxnProcessor::CheckWrites(Txn* txn) {

  for (set<Key>::iterator it = txn->writeset_.begin();
     it != txn->writeset_.end(); ++it) {

    Version * result = NULL;
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->reads_[*it] = result;

      if (!storage_->CheckWrite(*it, result, txn)) {
        return false;
      }
    }
  }
  return true;

}

void TxnProcessor::FinishWrites(Txn* txn) {

  for (map<Key, Version*>::iterator it = txn->writes_.begin();
     it != txn->writes_.end(); ++it) {

    // first is pointer to version, 2nd is txn
    storage_->FinishWrite(it->first, it->second);

  }

}

void TxnProcessor::PutEndTimestamps(Txn* txn) {

  for (map<Key, Version*>::iterator it = txn->writes_.begin();
     it != txn->writes_.end(); ++it) {

    // first is the old version, 2nd is new version
    storage_->PutEndTimestamp(txn->reads_[it->first], txn->writes_[it->first], txn->end_unique_id_);

  }

}

void TxnProcessor::SnapshotExecuteTxn(Txn* txn) {

  // Begin stage
  GetBeginTimestamp(txn);

  // Normal execution stage
  GetReads(txn);

  if (!CheckWrites(txn))
    txn->status_ = ABORTED;
  
  // Preparing stage
  if (txn->Status() == ACTIVE) {
    txn->Run();
    if (txn->Status() != ABORTED) {
      FinishWrites(txn);
      GetEndTimestamp(txn);
    }
  }
    
  // Postprocessing Phase
  if (txn->Status() == COMMITTED){
    PutEndTimestamps(txn);
    txn_results_.Push(txn);
  }
  else if(txn->Status() == ABORTED) {
    //TODO: cleanup txn
    // set begin field of its new versions to infinity 

    // atomically attempts to set end field of old versions to infinity

    txn->reads_.empty();
    txn->writes_.empty();
    // txn->status_ = INCOMPLETE;
    txn_requests_.Push(txn);
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

/////////////////////// END OF SNAPSHOT EXECUTION /////////////////////////////

void TxnProcessor::RunNewScheduler() {
  return;
}


