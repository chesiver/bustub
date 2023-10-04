#include <cstdio>
#include <random>
#include <vector>

#include "storage/disk/disk_manager.h"
#include "buffer/buffer_pool_manager_instance.h"
#include "storage/index/b_plus_tree.h"
#include "test_util.h"

using namespace bustub;

void InsertHelper(BPlusTree<GenericKey<8>, RID, GenericComparator<8>> *tree, const std::vector<int64_t> &keys,
                  __attribute__((unused)) uint64_t thread_itr = 0) {
  GenericKey<8> index_key;
  RID rid;
  // create transaction
  auto *transaction = new Transaction(0);
  for (auto key : keys) {
    int64_t value = key & 0xFFFFFFFF;
    rid.Set(static_cast<int32_t>(key >> 32), value);
    index_key.SetFromInteger(key);
    tree->Insert(index_key, rid, transaction);
  }
  delete transaction;
}

std::vector<int64_t> GenerateRandomKeys(size_t size, int maximum, std::string file_name = "./temp") {
  std::random_device device;
  std::mt19937 rng(device());
  std::uniform_int_distribution<std::mt19937::result_type> uni(1, maximum);
  std::vector<int64_t> keys;
  for (size_t i = 0; i < size; i += 1) {
    keys.push_back(i);
  }
  return keys;
}

int main() {
  // create KeyComparator and index schema
  auto key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema.get());

  FMT_MAYBE_UNUSED auto *disk_manager = new DiskManager("test.db");

  BufferPoolManager *bpm = new BufferPoolManagerInstance(50, disk_manager);

  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm, comparator, 254, 254);

  // create transaction
  auto *transaction = new Transaction(0);

  // create and fetch header_page

  page_id_t page_id;
  auto header_page = bpm->NewPage(&page_id);
  (void)header_page;

  size_t initial_size = 1000000;
  std::vector<int64_t> keys = GenerateRandomKeys(initial_size, 100000);

  InsertHelper(&tree, keys);

  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete transaction;
  delete disk_manager;
  delete bpm;
  remove("test.db");
  remove("test.log");

  return 0;
}