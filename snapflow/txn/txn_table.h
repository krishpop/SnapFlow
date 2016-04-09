#ifndef _TXN_TABLE_H_
#define _TXN_TABLE_H_

#include <map>
#include "txn/txn.h"
#include "utils/atomic.h"

// This is a table that holds the transactions that
// have WRITTEN/TRIED TO WRITE to the database.
class TxnTable {
	public:
		TxnTable() {}
		~TxnTable() {}

		void AddToTable(int, Txn*);

		Txn* ReadTable(int);

	private:
		AtomicMap<int, Txn*> txn_table;
};





#endif  // _TXN_PROCESSOR_H_