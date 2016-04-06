// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

  if (mvcc_data_.count(key)) {
    deque<Version*> * data_versions_p =  mvcc_data_[key];

    // This works under the assumption that we have the deque sorted in decreasing order
    Version *right_version = NULL;
    for (deque<Version*>::iterator it = data_versions_p->begin();
      it != data_versions_p->end(); ++it) {
      if (((*it)->version_id_ <= txn_unique_id)) {
          right_version = (*it);
          break;
      }
    }
    

    if (right_version == NULL) {
      return false;
    }
    else {
      if (right_version->max_read_id_ < txn_unique_id) {
        right_version->max_read_id_ = txn_unique_id;
      }
      *result = right_version->value_;

    }

  }
  else {
    return false;
  }
  return true;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  
  if (mvcc_data_.count(key)) {
    // Obtain the most recent version before txn_unique_id
    deque<Version*> * data_versions_p =  mvcc_data_[key];
    Version *most_recent_version = NULL;
    for (deque<Version*>::iterator it = data_versions_p->begin();
      it != data_versions_p->end(); ++it) {

      // Here we assume that the deque is sorted in decreasing order
      if (((*it)->version_id_ <= txn_unique_id)) {
        if(txn_unique_id >= (*it)->max_read_id_) {
          most_recent_version = (*it);
          break;
        }
        else {
          return false;
        }
      }
        
    }
    if (most_recent_version->max_read_id_ > txn_unique_id) {
      return false;
    }

  }

  return true;
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  Version * to_insert = new Version{ value, 0, txn_unique_id };

  if (!mvcc_data_.count(key)) {
    mvcc_data_[key] = new deque<Version*>();
    mvcc_data_[key]->push_back(to_insert);
  }
  else {
    deque<Version*> * data_p = mvcc_data_[key];
    // We have to insert into the sorted order
    for(deque<Version*>::iterator it = data_p->begin();
      it != data_p->end(); ++it) {
      // If we have reached a point to insert
      // Not sure if we have to insert when the id is ==
      if ((*it)->version_id_ <= txn_unique_id) {
        it = data_p->insert(it, to_insert);
        return;
      }
    }

    data_p->push_back(to_insert);
  }


  
  
}


