// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi, David Marcano, Krishnan Srinivasan, Kshitijh Meelu, Jeremy Liu

#include "txn/mvcc_storage.h"

// Init the storage
void MVCCStorage::InitStorage() {
  mvcc_data_.push_back(InitTable()); // Table for checking
  mvcc_data_.push_back(InitTable()); // Table for savings
}

// Init the table
unordered_map<Key, deque<Version*>*> MVCCStorage::InitTable() {
  unordered_map<Key, deque<Version*>*> table_;

  for (int i = 0; i < 1000000;i++) {
    table_[i] = new deque<Version*>();
    Timestamp begin_ts = Timestamp{ 0, NULL, 0};
    Timestamp end_ts = Timestamp{ INF_INT, NULL, 0};

    Version* to_insert = new Version;
    to_insert->value_ = 0;
    to_insert->begin_id_ = begin_ts;
    to_insert->end_id_ = end_ts;

    table_[i]->push_front(to_insert);
  }

  return table_;
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  // clear checking table
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_[CHECKING].begin();
       it != mvcc_data_[CHECKING].end(); ++it) {
    delete it->second;
  }

  // clear savings table
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_[SAVINGS].begin();
       it != mvcc_data_[SAVINGS].end(); ++it) {
    delete it->second;
  }

  // clear storage
  mvcc_data_.clear();

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

uint64 MVCCStorage::GetBeginTimestamp(Version * v, uint64 my_id, Timestamp & ts) {
  // What if ts.txn has committed and replaced itself?
  // This requires that the txn keeps its pointer in the Txn field of the TS
  ts.mutex_.Lock();
  Txn * txn_p = (Txn*) ts.txn;
  ts.mutex_.Unlock();
  int status = txn_p->Status();
  uint64 id = txn_p->GetStartID();

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
  // We could change the return depending on when the TS is "propagated".
  else if (status == COMMITTED) {
    return txn_p->GetEndID();
  }
  else if (status == ABORTED) {
    return INF_INT;
  }
  return INF_INT;

}

uint64 MVCCStorage::GetEndTimestamp(Version * v, uint64 my_id, Timestamp & ts) {
  // What if ts.txn has committed and replaced itself?
  // This requires that the txn keeps its pointer in the Txn field of the TS
  ts.mutex_.Lock();
  Txn * txn_p = (Txn*) ts.txn;
  ts.mutex_.Unlock();
  // We get a segfault here since txn_p is NULL. This can happen if the edit_bit_ has been
  // set by another thread BUT we reach the lock first.
  TxnStatus status = txn_p->Status();

  if (status == ACTIVE) {
      return INF_INT;
  }
  else if (status == COMMITTED) {
    return txn_p->GetEndID();
  }
  else if (status == ABORTED) {
    return INF_INT;
  }
  return INF_INT;

}


bool MVCCStorage::Read(Key key, Version** result, uint64 txn_unique_id, const TableType tbl_type) {
  if (mvcc_data_[tbl_type].count(key)) {
    deque<Version*> * data_versions_p =  mvcc_data_[tbl_type][key];
    uint64 begin_ts, end_ts;
    // This works under the assumption that we have the deque sorted in decreasing order
    Version *right_version = NULL;
    for (deque<Version*>::iterator it = data_versions_p->begin();
      it != data_versions_p->end(); ++it) {

      // Case 2:
      if (*((*it)->begin_id_.edit_bit) == 1) {
        begin_ts = GetBeginTimestamp(*it, txn_unique_id, (*it)->begin_id_);
        if (!(*((*it)->end_id_.edit_bit) == 1)) {
          end_ts = (*it)->end_id_.timestamp;
        }
        // Case 3:
        else {
          end_ts = GetEndTimestamp(*it, txn_unique_id, (*it)->end_id_);
        }
      }
      else {
        begin_ts = (*it)->begin_id_.timestamp;
        // Case 3:
        if (*((*it)->end_id_.edit_bit) == 1) {
          end_ts = GetEndTimestamp(*it, txn_unique_id, (*it)->end_id_);
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
      *result = right_version;
      return true;
    }
  }
  else {
    return false;
  }
  return true;
}

// TODO: Change the end timestamp of old version, flip the bit, change
// the begin timsteamp of new version and flip bit.
void MVCCStorage::PutEndTimestamp(Version * old_version, Version * new_version, uint64 ts) {
  old_version->end_id_.timestamp = ts;
  new_version->begin_id_.timestamp = ts;

  --old_version->end_id_.edit_bit;
  --new_version->begin_id_.edit_bit;

}

// MVCC CheckWrite returns true if Write without conflict
bool MVCCStorage::CheckWrite(Key key, Version* read_version, Txn* current_txn, const TableType tbl_type) {
  deque<Version*> * data_p = mvcc_data_[tbl_type][key];
  Version * front = data_p->front();

  // TODO: Need to check read_version with front?

  // mutex locks critical section of acquiring write priviledge
  front->end_id_.mutex_.Lock();
  if (front->end_id_.edit_bit == 0) {
    front->end_id_.edit_bit == 1;
    front->end_id_.txn = current_txn;
    front->end_id_.mutex_.Unlock();
    // We leave the timestamp in the front->end_id_.timestamp as INF_INT
    return true;
  }
  else {
    Txn* old_txn = (Txn*)front->end_id_.txn;
    if (old_txn && old_txn->Status() == ABORTED) {
      front->end_id_.txn = (void*) current_txn;
      front->end_id_.mutex_.Unlock();
      return true;
    }
    else {
      front->end_id_.mutex_.Unlock();
      return false;
    }
  }
  return false;
}

void MVCCStorage::FinishWrite(Key key, Version* new_version, const TableType tbl_type) {
  deque<Version*> * data_p = mvcc_data_[tbl_type][key];
  data_p->push_front(new_version);
  return;
}
