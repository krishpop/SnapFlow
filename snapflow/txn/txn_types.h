// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#ifndef _TXN_TYPES_H_
#define _TXN_TYPES_H_

#include <map>
#include <set>
#include <string>

#include "txn/edgegraph.h"
#include "txn/txn.h"

// Immediately commits.
class Noop : public Txn {
 public:
  Noop() {}
  virtual void Run() { }//COMMIT; }

  Noop* clone() const {             // Virtual constructor (copying)
    Noop* clone = new Noop();
    this->CopyTxnInternals(clone);
    return clone;
  }
};

// Reads all keys in the map 'm', if all results correspond to the values in
// the provided map, commits, else aborts.
class Expect : public Txn {
 public:
  Expect(const map<Key, Value>& m) : m_(m) {
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it)
      readset_.insert(it->first);
  }

  Expect* clone() const {             // Virtual constructor (copying)
    Expect* clone = new Expect(map<Key, Value>(m_));
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    Value result;
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it) {
      // If we didn't find it, this txn is doomed to fail, do not retry.
      if (!Read(it->first, &result)) {
        ABORT;
      }
      // If we find it and there is another value => write skew problem?
      // else if (result != it->second) {
      //   printf("write-skew?\n");
      // }
    }
    //COMMIT;
  }

 private:
  map<Key, Value> m_;
};

// Inserts all pairs in the map 'm'.
class Put : public Txn {
 public:
  Put(const map<Key, Value>& m) : m_(m) {
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it)
      writeset_.insert(it->first);
  }

  Put* clone() const {             // Virtual constructor (copying)
    Put* clone = new Put(map<Key, Value>(m_));
    this->CopyTxnInternals(clone);
    return clone;
  }

  virtual void Run() {
    for (map<Key, Value>::iterator it = m_.begin(); it != m_.end(); ++it) {
      Version * to_insert = new Version;
      Write(it->first, it->second, to_insert);
    }
    //COMMIT;
  }

 private:
  map<Key, Value> m_;
};

// Read-modify-write transaction.
class RMW : public Txn {
 public:
  explicit RMW(double time = 0) : time_(time) {}
  RMW(const set<Key>& writeset, double time = 0) : time_(time) {
    writeset_ = writeset;
  }
  RMW(const set<Key>& readset, const set<Key>& writeset, double time = 0)
      : time_(time) {
    readset_ = readset;
    writeset_ = writeset;
  }

  // Constructor with randomized read/write sets
  RMW(int dbsize, int readsetsize, int writesetsize, double time = 0)
      : time_(time) {
    // Make sure we can find enough unique keys.
    DCHECK(dbsize >= readsetsize + writesetsize);

    // Find readsetsize unique read keys.
    for (int i = 0; i < readsetsize; i++) {
      Key key;
      do {
        key = rand() % dbsize;
      } while (readset_.count(key));
      readset_.insert(key);
    }

    // Find writesetsize unique write keys.
    for (int i = 0; i < writesetsize; i++) {
      Key key;
      do {
        key = rand() % dbsize;
      } while (readset_.count(key) || writeset_.count(key));
      writeset_.insert(key);
    }
  }

  RMW* clone() const {             // Virtual constructor (copying)
    RMW* clone = new RMW(time_);
    this->CopyTxnInternals(clone);
    return clone;
  }

  void ReadWriteTable(const TableType& table) {
    // Read everything in readset.
    for (set<Key>::iterator it = readset_[table].begin(); it != readset_[table].end(); ++it) {
      Read(*it, &result, table);
    }

    // Increment length of everything in writeset.
    for (set<Key>::iterator it = writeset_[table].begin(); it != writeset_[table].end();
         ++it) {
      Version * to_insert = new Version;
      result = 0;
      Read(*it, &result, table);
      Write(*it, result + 1, to_insert, table);
    }
  }

  virtual void Run() {
    Value result;
    TableType table = CHECKING;
    // Execute everything in our read/write sets for CHECKING
    ReadWriteTable(table);
    // Now do the same for the SAVINGS table
    table = SAVINGS;
    ReadWriteTable(table);

    // Run while loop to simulate the txn logic(duration is time_).
    double begin = GetTime();
    while (GetTime() - begin < time_) {
      for (int i = 0;i < 1000; i++) {
        int x = 100;
        x = x + 2;
        x = x*x;
      }
    }

    // For SI we do not want to set the status at txn execution time.
    //COMMIT;
  }

 private:
  double time_;
};


// WriteCheck txns to deal with write-skew (used by a Checking/Savings system)
class WriteCheck : public Txn {
 public:
  explicit WriteCheck(double time = 0) : time_(time) {}
  WriteCheck(const set<Key>& writeset, double time = 0) : time_(time) {
    writeset_ = writeset;
  }
  WriteCheck(const set<Key>& readset, const set<Key>& writeset, double time = 0)
      : time_(time) {
    readset_ = readset;
    writeset_ = writeset;
  }

  // Constructor with randomized read/write sets
  WriteCheck(int dbsize, int readsetsize, int writesetsize, double time = 0)
      : time_(time) {
    // Make sure we can find enough unique keys.
    DCHECK(dbsize >= readsetsize + writesetsize);

    // Find readsetsize unique read keys.
    for (int i = 0; i < readsetsize; i++) {
      Key key;
      do {
        key = rand() % dbsize;
      } while (readset_.count(key));
      readset_.insert(key);
    }

    // Find writesetsize unique write keys.
    for (int i = 0; i < writesetsize; i++) {
      Key key;
      do {
        key = rand() % dbsize;
      } while (readset_.count(key) || writeset_.count(key));
      writeset_.insert(key);
    }
  }

  WriteCheck* clone() const {             // Virtual constructor (copying)
    WriteCheck* clone = new WriteCheck(time_);
    this->CopyTxnInternals(clone);
    return clone;
  }

  // Just readset?
  // This functions checks our readset again and creates a constraint checking
  // struct and compares it with the original one. Returns true if they're
  // the same, returns false otherwise.
  virtual bool Validate() {
    vector<bool> val_path;
    for (set<Key>::iterator it = readset_.begin(), vector<bool>::iterator it_v = path_.begin();
     it != readset_.end(); ++it, ++it_v) {
      if (!ValidatePath(*it, *it_v, val_path)) { // This must be done before committing, right?
        return false;
      }
    }
    return true;
  }

  // Constructs a path and compares with path_ while doing so.
  // Returns true if the constructed path matches with path_'s, otherwise
  // returns false.
  bool ValidatePath(const Key& key, const bool& path_value, vector<bool> val_path) {
    Value result_chk;
    Value result_sav;
    Read(key, &result_chk) // read from checking TODO
    Read(key, &result_sav) // read from savings TODO
    if (result_sav + result_chk >= constraint_) {
      // Do necessary operations with V TODO
      if (path_value == true) {
        val_path.push_back(true);
      }
      else {
        return false;
      }
    }
    else {
      // Do necessary operations with V TODO
      if (path_value == false) {
        val_path.push_back(false);
      }
      else {
        return false;
      }
    }
    return true;
  }

  // Note that this function is dependent on the constraint that we
  // are checking. To add more constraints, this and ValidatePath would
  // have to change.
  void ConstructPath(const Key& key, const Value& val, const TableType& table) {
    Value result_chk;
    Value result_sav;
    Read(key, &result_chk, CHECKING); // read from checking TODO
    Read(key, &result_sav, SAVING); // read from savings TODO
    if (result_sav + result_chk >= constraint_) {
        path_.push_back(true);
    }
    else {
        path_.push_back(false);
    }
  }

  void ReadWriteTable(const TableType& table) {
    // Read everything in readset.
    for (set<Key>::iterator it = readset_[table].begin(); it != readset_[table].end(); ++it) {
      Read(*it, &result, table);
      // We already read one result in, we need only read in from the other table
      ConstructPath(*it, result, table);
    }

    // Increment length of everything in writeset.
    for (set<Key>::iterator it = writeset_[table].begin(); it != writeset_[table].end();
         ++it) {
      Version * to_insert = new Version;
      result = 0;
      Read(*it, &result, table);
      Write(*it, result + 1, to_insert, table);
    }
  }

  // TODO: update this Run function to create some kind of struct that checks
  // the path taken through the constraint checks.
  virtual void Run() {
    Value result;
    TableType table = CHECKING;
    // Execute everything in our read/write sets for CHECKING
    ReadWriteTable(table);
    // Now do the same for the SAVINGS table
    table = SAVINGS;
    ReadWriteTable(table);

    // Run while loop to simulate the txn logic(duration is time_).
    double begin = GetTime();
    while (GetTime() - begin < time_) {
      for (int i = 0;i < 1000; i++) {
        int x = 100;
        x = x + 2;
        x = x*x;
      }
    }

    // For SI we do not want to set the status at txn execution time.
    //COMMIT;
  }

 private:
  double time_;
  // This is the path of the binary (for now) tree that we must compare
  // against when validating for write-skew.
  vector<bool> path_;
  // For WriteCheck txns, constraint_ is the upper bound on how much
  // money the customer must have for the txn to not penalize him/her.
  Value constraint_;
};

#endif  // _TXN_TYPES_H_
