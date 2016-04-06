// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  LockMode ex = EXCLUSIVE;
  if (!lock_table_.count(key))
    lock_table_[key] = new deque<LockRequest>(); // need to free in destruct

  lock_table_[key]->push_back(LockRequest(ex, txn));
  
  // If this is the first entry in the deque, give the lock immediately
  if (lock_table_[key]->size() == 1) {
    // If this transaction was waiting for this lock, decrease the waiting counter
    // if (txn_waits_.count(txn)) {
    //   if (txn_waits_[txn] > 0) {
    //     --(txn_waits_[txn]);
    //   }
      
    // }
    
    return true;
  }
  else {
    ++(txn_waits_[txn]);
    return false;
  }
  
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}


void LockManagerA::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

  // Can a transaction be in the same deque more than once in the lock_table?


  // If the first transaction in the lock_table_ deque is txn, then we erase it and
  // grant the lock to the next transaction.
  // for(unordered_map<Txn*, int>::const_iterator it = txn_waits_.begin();
  //   it != txn_waits_.end(); ++it) {
  //   std::cout << "txn_waits_:" << it->first << " " << it->second << "\n";
  // }
  if (lock_table_.count(key)) {
    deque<LockRequest> * lock_requests_p = lock_table_[key];
    Txn * lock_owner = lock_requests_p->front().txn_;
    deque<LockRequest>::iterator it = lock_requests_p->begin();
    if (lock_owner == txn) {
      // No need to decrease txn_waits_ because the first one is not waiting for the lock
      it = lock_requests_p->erase(it);
      if (lock_requests_p->size() > 0) {
        lock_owner = it->txn_;
        // Reduce the number of locks still needed by this txn.
        if (txn_waits_.count(lock_owner)) {
          if (txn_waits_[lock_owner]) {
            --(txn_waits_[lock_owner]);
          }
        }
        // If the new first transaction has now acquired all locks:
        if (txn_waits_[lock_owner] == 0) {
          ready_txns_->push_back(lock_owner);
          // perhaps delete from txn_waits_?
        }
      }
      

    }
    // We remove all locks for this transaction on key
    // If we erase a txn from the lock_table_, we should decrement txn_waits_, right?
    if (lock_requests_p->size() > 0) {
      if (lock_table_.count(key)) { //not the right check? This only counts the keys
        while(it != lock_requests_p->end()) {
          if (it->txn_ == txn) {
            // we should never enter here if it is true that the same deque cannot
            // contain the same element AND it was the first element removed above
            if (txn_waits_.count(it->txn_)) {
              if (txn_waits_[it->txn_]) {
                --(txn_waits_[it->txn_]);

                //Should we make "ready" a transaction that has called Release?
              }
            }
            it = lock_requests_p->erase(it);
          }
          else {
            ++it;
          }
        }
      }
    }
  }
  // for(unordered_map<Txn*, int>::const_iterator it = txn_waits_.begin();
  //   it != txn_waits_.end(); ++it) {
  //   std::cout << "txn_waits_:" << it->first << " " << it->second << "\n";
  // }

}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
  // NEED TO TEST IF owners IS A NULL POINTER

  // clear owners vector -- Seems like a break in function encapsulation
  // could use vector.clear();
  if (owners->size() != 0) {
    vector<Txn*>::iterator it_v = owners->begin();
    while(!owners->empty()) {
      it_v = owners->erase(it_v);
    }
  }
  

  if (lock_table_.count(key)) {
    deque<LockRequest> * lock_requests_p = lock_table_[key];
    deque<LockRequest>::iterator it = lock_requests_p->begin();
    bool nshared_locks = false;
    if (it->mode_ == EXCLUSIVE) {
        owners->push_back(it->txn_);  // Can we access this?
        return EXCLUSIVE;
    }

    // go until we hit an EXCLUSIVE or we hit the end.
    for (;it != lock_requests_p->end() && it->mode_ != EXCLUSIVE; ++it){
      // put all SHARED Ids into owners, if any

      owners->push_back(it->txn_); // Can we access this?
      nshared_locks = true;
    }

    if (nshared_locks) {
      return SHARED;
    }
    else {
      return UNLOCKED;
    }
  }

  return UNLOCKED;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  LockMode ex = EXCLUSIVE;
  if (!lock_table_.count(key))
    lock_table_[key] = new deque<LockRequest>(); // need to free in destruct

  lock_table_[key]->push_back(LockRequest(ex, txn));
  
  // If this is the first entry in the deque, give the lock immediately
  if (lock_table_[key]->size() == 1) {
    // If this transaction was waiting for this lock, decrease the waiting counter
    // if (txn_waits_.count(txn) > 0) {
    //   if (txn_waits_[txn] > 0) {
    //     --(txn_waits_[txn]);
    //   }
      
    // }
    
    return true;
  }
  else {
    ++(txn_waits_[txn]);
    return false;
  }
}

// Need to modify this
bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!
  LockMode ex = SHARED;
  if (!lock_table_.count(key))
    lock_table_[key] = new deque<LockRequest>(); // need to free in destruct

  lock_table_[key]->push_back(LockRequest(ex, txn));
  
  // If all the owners of the lock are SHARED, then grant this one the lock.
  vector<Txn*> owners;
  vector<Txn*> * owners_p = &owners;

  // We only grant the lock if the lock is shared AND the new txn is now part of the owners set.
  if ((Status(key, owners_p) == SHARED && find_vector(txn, owners_p)) || Status(key, owners_p) == UNLOCKED) {
    // If this transaction was waiting for this lock, decrease the waiting counter
    // if (txn_waits_.count(txn) > 0) {
    //   if (txn_waits_[txn] > 0) {
    //     --(txn_waits_[txn]);
    //   }
      
    // }
    
    return true;
  }
  else {
    ++(txn_waits_[txn]);
    return false;
  }
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // CPSC 438/538:
  //
  // Implement this method!

    // If the first transaction in the lock_table_ deque is txn, then we erase it and
  // grant the lock to the next transaction.
  // for(unordered_map<Txn*, int>::const_iterator it = txn_waits_.begin();
  //   it != txn_waits_.end(); ++it) {
  //   std::cout << "txn_waits_: " << it->first << " " << it->second << "\n";
  // }
  vector<Txn*> orig_owners;
  vector<Txn*> new_owners;

  if (lock_table_.count(key)) {
    Status(key, &orig_owners);
    deque<LockRequest>::iterator it = lock_table_[key]->begin();
    while(it != lock_table_[key]->end()) {
      if (it->txn_ == txn) {
        it = lock_table_[key]->erase(it);
        // should we push into ready_txns or decrement txn_waits_ txns that are releasing keys?
        // I don't think we should...
        // But now a transaction that has released a lock is still waiting for a lock in txn_waits_?
      }
      else {
        ++it;
      }
    }
    Status(key, &new_owners);
    vector<Txn*>::iterator it_n = new_owners.begin();
    for (;it_n != new_owners.end();++it_n) {

      if (!find_vector(*it_n, &orig_owners)) {
        if (txn_waits_.count(*it_n)) {
          if (txn_waits_[*it_n] > 0) {
            --(txn_waits_[*it_n]);
          }
          if (txn_waits_[*it_n] == 0) {
            ready_txns_->push_back(*it_n);
          }
        }
        
      }
    }
  }
  

  // for(unordered_map<Txn*, int>::const_iterator it = txn_waits_.begin();
  //   it != txn_waits_.end(); ++it) {
  //   std::cout << "txn_waits_:" << it->first << " " << it->second << "\n";
  // }

}

bool LockManagerB::find_vector(const Txn * txn, const vector<Txn*> * owners) {
  for(vector<Txn*>::const_iterator it_v = owners->begin(); it_v != owners->end(); ++it_v) {
    if (txn == *it_v) {
      return true;
    }
  }
  return false;
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // CPSC 438/538:
  //
  // Implement this method!
    // clear owners vector -- Seems like a break in function encapsulation
  // could use vector.clear();
  if (owners->size() != 0) {
    vector<Txn*>::iterator it_v = owners->begin();
    while(!owners->empty()) {
      it_v = owners->erase(it_v);
    }
  }
  

  if (lock_table_.count(key)) {
    deque<LockRequest> * lock_requests_p = lock_table_[key];
    deque<LockRequest>::iterator it = lock_requests_p->begin();
    bool nshared_locks = false;
    if (it->mode_ == EXCLUSIVE) {
        owners->push_back(it->txn_);  // Can we access this?
        return EXCLUSIVE;
    }

    // go until we hit an EXCLUSIVE or we hit the end.
    for (;it != lock_requests_p->end() && it->mode_ != EXCLUSIVE; ++it){
      // put all SHARED Ids into owners, if any

      owners->push_back(it->txn_); // Can we access this?
      nshared_locks = true;
    }

    if (nshared_locks) {
      return SHARED;
    }
    else {
      return UNLOCKED;
    }
  }

  return UNLOCKED;
}

