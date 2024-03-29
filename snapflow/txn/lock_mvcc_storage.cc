// Author: SNAPFLOW BOYS

#include "txn/lock_mvcc_storage.h"

LockMVCCStorage::~LockMVCCStorage() {
  // clear checking table
  for (unordered_map<Key, deque<Version*>*>::iterator it = lock_mvcc_data_[CHECKING].begin();
       it != lock_mvcc_data_[CHECKING].end(); ++it) {
    delete it->second;
  }

  // clear savings table
  for (unordered_map<Key, deque<Version*>*>::iterator it = lock_mvcc_data_[SAVINGS].begin();
       it != lock_mvcc_data_[SAVINGS].end(); ++it) {
    delete it->second;
  }

  // clear storage
  lock_mvcc_data_.clear();

  for (unordered_map<Key, Mutex*>::iterator it = mutexs_[CHECKING].begin();
       it != mutexs_[CHECKING].end(); ++it) {
    delete it->second;
  }

  for (unordered_map<Key, Mutex*>::iterator it = mutexs_[SAVINGS].begin();
       it != mutexs_[SAVINGS].end(); ++it) {
    delete it->second;
  }

  mutexs_.clear();

}

bool LockMVCCStorage::Read(Key key, Version** result, uint64 txn_unique_id, const TableType tbl_type, const bool& val) {
  if (lock_mvcc_data_[tbl_type].count(key)) {
    deque<Version*>* data_versions_p = lock_mvcc_data_[tbl_type][key];
    for (deque<Version*>::iterator it = data_versions_p->begin();
         it != data_versions_p->end(); ++it) {
      if ((*it)->version_id_ < txn_unique_id){
        if (txn_unique_id > (*it)->max_read_id_)
          (*it)->max_read_id_ = txn_unique_id;
        *result = *it;
        return true;
      }
    }
  }
  return false;
}

void LockMVCCStorage::Lock(Key key, const TableType tbl_type) {
  mutexs_[tbl_type][key]->Lock();
}

void LockMVCCStorage::Unlock(Key key, const TableType tbl_type) {
  mutexs_[tbl_type][key]->Unlock();
}

bool LockMVCCStorage::LockCheckWrite(Key key, uint64 txn_unique_id, const TableType tbl_type) {

  if (lock_mvcc_data_[tbl_type].count(key)) {
    deque<Version*>* data_versions_p = lock_mvcc_data_[tbl_type][key];

    for (deque<Version*>::iterator it = data_versions_p->begin();
         it != data_versions_p->end(); ++it) {
      if ((*it)->version_id_ < txn_unique_id) {
        if ((*it)->max_read_id_ <= txn_unique_id)
          return true;
        else
          return false;
      }
    }
  }
  return true;
}

void LockMVCCStorage::FinishWrite(Key key, Version* new_version, const TableType tbl_type) {

  if (lock_mvcc_data_[tbl_type].count(key)) {
    deque<Version*>* data_versions_p = lock_mvcc_data_[tbl_type][key];

    for (deque<Version*>::iterator it = data_versions_p->begin();
         it != data_versions_p->end(); ++it) {
      if ((*it)->version_id_ <= new_version->version_id_) {
        it = data_versions_p->insert(it, new_version);
        return;
      }
    }
    data_versions_p->push_back(new_version);
  }
  else {
    DIE("Unable to FinishWrite bc missing key");
  }
}

void LockMVCCStorage::InitStorage() {
  TableType tbl = CHECKING;
  unordered_map<Key, Mutex*> temp1;
  unordered_map<Key, Mutex*> temp2;
  mutexs_.push_back(temp1);
  mutexs_.push_back(temp2);
  lock_mvcc_data_.push_back(InitTable(tbl)); // Table for checking
  tbl = SAVINGS;
  lock_mvcc_data_.push_back(InitTable(tbl)); // Table for savings
}

unordered_map<Key, deque<Version*>*> LockMVCCStorage::InitTable(TableType tbl) {

  unordered_map<Key, deque<Version*>*> table_;
  // TODO: set to 1000000
  for (int i = 0; i < 1000000; ++i) {
    table_[i] = new deque<Version*>();
    Timestamp begin_ts = Timestamp{ 0, NULL, 0};
    Timestamp end_ts = Timestamp{ INF_INT, NULL, 0};

    Version* to_insert = new Version;
    if (tbl == SAVINGS) {
      to_insert->value_ = 5;
    }
    else {
      to_insert->value_ = 0;
    }
    to_insert->begin_id_ = begin_ts;
    to_insert->end_id_ = end_ts;
    to_insert->version_id_ = 0;
    to_insert->max_read_id_ = 0;
    mutexs_[tbl][i] = new Mutex();

    table_[i]->push_front(to_insert);
  }

  return table_;

}
