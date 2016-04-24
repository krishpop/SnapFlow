// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "txn/txn.h"

#include <string>

#include "txn/txn_processor.h"
#include "txn/txn_types.h"
#include "utils/testing.h"

TEST(NoopTest) {
  TxnProcessor p(SI);

  Txn* t = new Noop();
  EXPECT_EQ(INCOMPLETE, t->Status());

  p.NewTxnRequest(t);
  p.GetTxnResult();

  EXPECT_EQ(COMMITTED, t->Status());
  delete t;

  END;
}

TEST(PutTest) {
  TxnProcessor p(SI);
  Txn* t;

  map<Key, Value> m;
  m[1] = 2;

  map<Key, Value> n;
  n[5] = 2;

  map<Key, Value> o;
  o[1] = 1;

  map<Key, Value> q;
  q[1] = 2;

  p.NewTxnRequest(new Put(m));
  p.GetTxnResult();

  p.NewTxnRequest(new Expect(n));  // Should abort (no key '0' exists)
  t = p.GetTxnResult();
  EXPECT_EQ(ABORTED, t->Status());
  //delete t;

  p.NewTxnRequest(new Expect(o));  // Should abort (wrong value for key)
  t = p.GetTxnResult();
  EXPECT_EQ(ABORTED, t->Status());
  //delete t;

  p.NewTxnRequest(new Expect(q));  // Should commit
  t = p.GetTxnResult();
  EXPECT_EQ(COMMITTED, t->Status());
  //delete t;

  END;
}

TEST(PutMultipleTest) {
  TxnProcessor p(SI);
  Txn* t;

  map<Key, Value> m;
  for (int i = 0; i < 1000; i++)
    m[i] = i*i;

  p.NewTxnRequest(new Put(m));
  delete p.GetTxnResult();

  p.NewTxnRequest(new Expect(m));
  t = p.GetTxnResult();
  EXPECT_EQ(COMMITTED, t->Status());
  delete t;

  END;
}

int main(int argc, char** argv) {
  NoopTest();
  PutTest();
  PutMultipleTest();

}

