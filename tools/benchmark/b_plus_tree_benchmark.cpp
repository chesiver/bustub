#include <cstdio>

#include "storage/disk/disk_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"

using namespace bustub;

int main() {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  FMT_MAYBE_UNUSED auto *disk_manager = new DiskManager("test.db");

  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 2, 3);
  GenericKey<8> index_key;
  RID rid;

  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  FMT_MAYBE_UNUSED std::vector<int64_t> keys;
  int64_t scale_factor = 100000;
  for (int64_t key = 1; key < scale_factor; key++) {
    keys.push_back(key);
  }

  for (auto key : keys) {
    printf("key: %lld\n", key);
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree.Insert(index_key, rid, transaction);
  }

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

  return 0;
}