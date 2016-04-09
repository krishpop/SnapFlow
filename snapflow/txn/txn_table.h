#ifndef _TXN_TABLE_H_
#define _TXN_TABLE_H_

#include <map>
#include "txn/txn.h"

class TxnTable {
	public:
		TxnTable() {}
		~TxnTable() {}
		
		void AddToTable(int, Txn*);

		Txn* ReadTable(int);

	private:
		unordered_map<int, Txn*> txn_table;
};





#endif  // _TXN_PROCESSOR_H_