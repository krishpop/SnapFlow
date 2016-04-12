// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)


#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode, TxnTable * t)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1), txn_table(t) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);
  
  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage(t);
  } else {
    storage_ = new Storage();
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
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;
    
  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
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
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }
          
      if (blocked == false) {
        // Request write locks.
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock(); 
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }
      
      // Release read locks.
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));

    }
  }
}

void TxnProcessor::ExecuteReadPhase(Txn* txn) {
    // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();
}

void TxnProcessor::ExecuteTxn(Txn* txn) {
  // do the Read phase
  ExecuteReadPhase(txn);
  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunOCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));
      // Now the result would be in completed_txns_?
    }
    // Validation phase for completed transactions:
    while(completed_txns_.Pop(&txn)) {
      bool validate = true;
      for(set<Key>::iterator it = txn->readset_.begin();
        it != txn->readset_.end(); ++it) {
        if (txn->occ_start_time_ < storage_->Timestamp(*it)) {
          validate = false;
          break;
        }
      }
      for(set<Key>::iterator it_w = txn->writeset_.begin();
        it_w != txn->writeset_.end(); ++it_w) {
        if (txn->occ_start_time_ < storage_->Timestamp(*it_w)) {
          validate = false;
          break;
        }
      }

      if (!validate) {
        cleanupRestart(txn);
      }
      else {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
        // Return result to client.
        txn_results_.Push(txn);
      }
    }

    
  }
}

void TxnProcessor::cleanupRestart(Txn* txn) {
  // cleanup txn
  txn->reads_.clear();
  txn->writes_.clear();
  txn->status_ = INCOMPLETE;

  // restart txn
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  ++next_unique_id_;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

void TxnProcessor::SerialValidate() {
  
}

// we check if current_txn writeset intersects with active_txn's read or writesets
bool TxnProcessor::writesetIntersects(Txn* active_txn, Txn* current_txn) {
  for(set<Key>::iterator it_w = current_txn->writeset_.begin();
    it_w != current_txn->writeset_.end(); ++it_w) {
    if (active_txn->readset_.count(*it_w) || active_txn->writeset_.count(*it_w)) {
      return true;
    }
  }
  return false;
}

// we check if current_txn readset intersects with active_txn's writeset
bool TxnProcessor::readsetIntersects(Txn* active_txn, Txn* current_txn) {
  for(set<Key>::iterator it_w = current_txn->readset_.begin();
    it_w != current_txn->readset_.end(); ++it_w) {
    if (active_txn->writeset_.count(*it_w)) {
      return true;
    }
  }
  return false;
}

void TxnProcessor::ParallelValidate(Txn* txn) {
  // Begin critical section

  // We make a "snapshot" of the active set since we only need to compare the
  // current txn with all txns that are concurrently validating/writing up to this point
  active_set_mutex_.Lock();
  set<Txn*> cpy_active_set = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();
  // End Critical section
  bool validate = true;
  for(set<Key>::iterator it = txn->readset_.begin();
    it != txn->readset_.end(); ++it) {
    if (txn->occ_start_time_ < storage_->Timestamp(*it)) {
      validate = false;
      break;
    }
  }

  for(set<Key>::iterator it_w = txn->writeset_.begin();
    it_w != txn->writeset_.end(); ++it_w) {
    if (txn->occ_start_time_ < storage_->Timestamp(*it_w)) {
      validate = false;
      break;
    }
  }

  for(set<Txn*>::iterator it = cpy_active_set.begin();
    it != cpy_active_set.end(); ++it) {
    // If txn's writeset does NOT intersect with *it's read AND writesets
    if (writesetIntersects(*it, txn)) {
      validate = false;
    }
    if (readsetIntersects(*it, txn)) {
      validate = false;
    }
  }

  if (validate) {
    ApplyWrites(txn);
    active_set_.Erase(txn);
    txn->status_ = COMMITTED;
    // Return result to client.
    txn_results_.Push(txn);
  }
  else {
    active_set_.Erase(txn);
    cleanupRestart(txn);
  }

}

void TxnProcessor::ExecuteTxnParallel(Txn* txn) {
  // Execute the read phase (fill the reads_ from the readset and writeset)
  ExecuteReadPhase(txn);
  // Validate this txn in parallel (with all the other txns validating)
  ParallelValidate(txn);
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxnParallel,
            txn));
    }
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn* txn) {
  // Read all necessary data from storage.
  // Read everything in from readset.


  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    storage_->Lock(*it);
    // We send a pointer to the txn_table
    if (storage_->Read(*it, &result, txn->unique_id_, &txn_table, txn)) {
      txn->reads_[*it] = result;
    }
    storage_->Unlock(*it);
      
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    storage_->Lock(*it);
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->reads_[*it] = result;
    }
    storage_->Unlock(*it);
      
  }

  // Execute txn's program logic.
  txn->Run();

  // Request write locks for everything in our writeset.
  // Also requests lock for things not in storage?
  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
      storage_->Lock(*it); // Definitely a bottleneck...    
  }
  bool success = true;
  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end() && success; ++it) {
      success = storage_->CheckWrite(*it, txn->unique_id_);
  }

  if (success) {
    ApplyWrites(txn);
    // Release all locks for keys in writeset
    for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
      storage_->Unlock(*it); 
    }
    // Do we need to set status to committed?
    txn->status_ = COMMITTED;
    // Return result to client.
    txn_results_.Push(txn);
  }
  else {
    // Release all locks for keys in writeset
    for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
      storage_->Unlock(*it);
    }
    cleanupRestart(txn);
  }

}

void TxnProcessor::RunMVCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute. 
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn. 
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]

  Txn* txn;
  while (tp_.Active()) {
    if (txn_requests_.Pop(&txn)) {
      // could be a possible bottleneck?
      //txn_table.AddToTable(txn->unique_id_, txn);
      // This doesn't work since we don't want to create a global
      // txn table. Instead we only want to add txns that WRITE
      // to the database. Thus the above should be in MVCCExecuteTxn.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::MVCCExecuteTxn,
            txn));
    }
  }
}

