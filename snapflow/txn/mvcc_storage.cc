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
 * Case 3: The end_id_active_ flag is set to true. In this case, we look at the
 * status currently operating on the end_id_ of this version. If Active, then
 *
 */

// It must be that t2 has already been added to txn_table
int MVCCStorage::GetBeginTimestamp(Version * v, int my_id, TimeStamp & ts) {
  TimeStamp new_ts;
  // What if ts.txn has committed and replaced itself?
  int status = ts.txn->GetStatus();
  int id = ts.txn->GetStartID();

<<<<<<< bccf669603b83464e48eb1247bf436ad97095923
uint64 MVCCStorage::GetBeginTimestamp(Version * v, int my_id, TimeStamp & ts) {
  // What if ts.txn has committed and replaced itself?
  // This requires that the txn keeps its pointer in the Txn field of the TS
  int status = ts.txn->GetStatus();
  uint64 id = ts.txn->GetStartID();

=======
>>>>>>> added the final bit flag for TS struct
  if (status == ACTIVE) {
    if (id == my_id && v->end_id_.timestamp == INF_INT) {
      // v is visible
      return my_id;
    }
    else {
      // v is not visible
      return INF_INT;
    }
  }
  // Since we're doing SI, we don't have to do this branch
  else if (status == PREPARING) {
    // We return the END ID since once we are in Preparing mode, we have already
    // acquired an END ID.
    return ts.txn->GetEndID();
  }
  // We could change the return depending on when the TS is "propagated".
  else if (status == COMPLETED_C) {
    return ts.txn->GetEndID();
  }
  else if (status == COMPLETED_A || status == ABORTED) {
    return INF_INT;
  }
  else {
    // the status is INCOMPLETE?
  }

}

uint64 MVCCStorage::GetEndTimestamp(Version * v, int my_id, TimeStamp & ts) {
  // What if ts.txn has committed and replaced itself?
  // This requires that the txn keeps its pointer in the Txn field of the TS
  int status = ts.txn->GetStatus();
  uint64 id = ts.txn->GetStartID();

  if (status == ACTIVE) {
      return INF_INT;
  }
  // Since we're doing SI, we don't have to do this branch
  else if (status == PREPARING) {
    // We return the END ID since once we are in Preparing mode, we have already
    // acquired an END ID.
    return ts.txn->GetEndID();
  }
  // We could change the return depending on when the TS is "propagated".
  else if (status == COMPLETED_C) {
    return ts.txn->GetEndID();
  }
  else if (status == COMPLETED_A || status == ABORTED) {
    return INF_INT;
  }
  else {
    // the status is INCOMPLETE?
  }

}

void MVCCStorage::InitTS(TimeStamp ts) {
  ts.timestamp = -1;
  ts.txn = NULL;
  ts.edit_bit = false;
}

bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {

  if (mvcc_data_.count(key)) {
    deque<Version*> * data_versions_p =  mvcc_data_[key];
    TimeStamp begin_ts, end_ts;
    // This works under the assumption that we have the deque sorted in decreasing order
    Version *right_version = NULL;
    for (deque<Version*>::iterator it = data_versions_p->begin();
      it != data_versions_p->end(); ++it) {

      InitTS(begin_ts);
      InitTS(end_ts);

      // Case 2:
      if ((*it)->begin_id_.edit_bit) {
        begin_ts = GetBeginTimestamp(*it, txn_unique_id, (*it)->begin_id_);
        if (!(*it)->end_id_.edit_bit) {
          end_ts = (*it)->end_id_.timestamp;
        }
        // Case 3:
        else {
          end_ts = GetEndTimestamp(*it, txn_unique_id);
        }
      }
      else {
        begin_ts = (*it)->begin_id_.timestamp;
        // Case 3:
        if ((*it)->end_id_.edit_bit) {
          end_ts = GetEndTimestamp(*it, txn_unique_id);
        }
        // Case 1:
        else {
          end_ts = (*it)->end_id_.timestamp;
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
  TimeStamp end_ts, begin_ts; // end_ts gets inserted into latest version, begin_ts into the new version
  InitTS(end_ts);
  InitTS(begin_ts);

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
