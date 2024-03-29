OBJDIRS += txn

# Create template specific variables
UPPERC_DIR := TXN
LOWERC_DIR := txn

TXN_SRCS := txn/mvcc_storage.cc txn/txn.cc txn/txn_processor.cc

SRC_LINKED_OBJECTS :=
TEST_LINKED_OBJECTS :=

# Link the template to avoid redundancy
include $(MAKEFILE_TEMPLATE)

# Need to specify test cases explicitly because they have variables in recipe
test-txn: $(TXN_TESTS)
	@for a in $(TXN_TESTS); do \
		echo == $$a ==; \
		$(LDLIBRARYPATH) $$a; \
	done
