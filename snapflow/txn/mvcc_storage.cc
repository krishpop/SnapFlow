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
/*Read for SI and SSI is more complex. Only certain versions are visible
 * to this txn based on its unique_id. 
 * Case 1: a version's being_id_ and end_id_ are established numbers. 
 * In this case we check to see if our
 * txn_unique_id is between those numbers. If it is, then we set result to
 * that value and return true. Otherwise we continue. (can different versions
 * have overlapping ranges?)
 *
 * Case 2: The begin_id_active_ flag is set to true. In this case, we have
 * to look up the begin_id_ up in a shared map table of running txns. We see
 * the status of that transaction. If Active, then that version is not visible
 * to us. If Preparing, then, we use end_unique_id_ as the begin timestamp and
 * use that version (specutively) if applicable. If Completed_C we use the
 * end_unique_id_ as the begin timestamp. If Completed_A, ignore that version.
 * If the txn is not found in the hashmap, recheck the begin_id_active_ flag,
 * proceed accordingly.
 * 
 * Case 3: 
 */

int GetBeginTimestamp(Version * v) {

}

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
      int begin_ts = -1, end_ts = -1;
      

      // Case 2:
      if ((*it)->begin_id_active_) {
        begin_ts = GetBeginTimestamp(*it);
        if (!(*it)->end_id_active_) {
          end_ts = (*it)->end_id_;
        }
        else {
          end_ts = GetEndTimestamp(*it);
        }
      }
      else {
        begin_ts = (*it)->begin_id_;
        // Case 3:
        if ((*it)->end_id_active_) {
          end_ts = GetEndTimestamp(*it);
        }
        // Case 1:
        else {
          end_ts = (*it)->end_id_;
        }
      } 
      

      // At the end, check using the timestamps found above:
      if ((begin_ts <= txn_unique_id) && (end_ts > txn_unique_id)) {
        right_version = (*it);
        break;
      }
    }
    if (right_version == NULL) {
      return false;
    }
    else {
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

