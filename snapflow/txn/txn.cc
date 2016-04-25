// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "txn/txn.h"
uint64 INF_INT = std::numeric_limits<uint64>::max();
bool Txn::Read(const Key& key, Value * value, const TableType& table, const bool& val) {
  // Check that key is in readset/writeset.
  if (readset_[table].count(key) == 0 && writeset_[table].count(key) == 0)
    DIE("Invalid read (key not in readset or writeset).");

  // Reads have no effect if we have already aborted or committed.
  if (status_ != INCOMPLETE && status_ != ACTIVE)
    return false;

  if (val) {
    *value = vals_[table][key]->value_;
    return true;
  }
  // If we have previously written to key, then we read the newest local
  // version.
  if (!val && writes_[table].count(key)) {
    *value = writes_[table][key]->value_;
    return true;
  }
  // 'reads_' has already been populated by TxnProcessor, so it should contain
  // the target value iff the record appears in the database.
  else if (reads_[table].count(key)) {
    *value = reads_[table][key]->value_;
    return true;
  } 
  else {
    return false;
  }
}

void Txn::Write(const Key& key, const Value& value, Version * to_insert, const TableType& table) {
  // Check that key is in writeset.
  if (writeset_[table].count(key) == 0)
    DIE("Invalid write to key " << key << " (writeset).");

  // Writes have no effect if we have already aborted or committed.
  if (status_ != INCOMPLETE && status_ != ACTIVE)
    return;

  Timestamp begin_ts = Timestamp{0, this, 1};
  Timestamp end_ts = Timestamp{INF_INT, NULL, 0};

  to_insert->value_ = value;
  to_insert->begin_id_ = begin_ts;
  to_insert->end_id_ = end_ts;
  // Set key-value pair in write buffer.
  writes_[table][key] = to_insert;

  // Also set key-value pair in read results in case txn logic requires the
  // record to be re-read.
  //reads_[key] = value;
}

// void Txn::CheckReadWriteSets() {
//   for (set<Key>::iterator it = writeset_.begin();
//        it != writeset_.end(); ++it) {
//     if (readset_.count(*it) > 0) {
//       DIE("Overlapping read/write sets\n.");
//     }
//   }
// }

void Txn::CopyTxnInternals(Txn* txn) const {
  txn->readset_ = vector<set<Key>>(this->readset_);
  txn->writeset_ = vector<set<Key>>(this->writeset_);
  txn->reads_ = vector<map<Key, Version*>>(this->reads_);
  txn->writes_ = vector<map<Key, Version*>>(this->writes_);
  txn->vals_ = vector<map<Key, Version*>>(this->vals_);
  txn->constraintset_ = set<Key>(this->constraintset_);
  txn->status_ = this->status_;
  txn->unique_id_ = this->unique_id_;
  txn->end_unique_id_ = this->end_unique_id_;
}
