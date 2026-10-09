#include <atomic>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <vector>

#include <dagflow/detail/runtime_memory.hpp>
#include "support.hpp"

struct alignas(256) Object {
  static inline std::atomic<int> alive{0};
  int value;
  explicit Object(int n = 0) : value(n) { ++alive; }
  ~Object() { --alive; }
};

struct ArrayElement {
  static inline int alive = 0;
  static inline int budget = 0;
  ArrayElement() {
    if (budget-- == 0) throw std::runtime_error("array construction");
    ++alive;
  }
  ~ArrayElement() { --alive; }
};

void array_storage() {
  CHECK(!dagflow::detail::make_owned_array<Object>(0));
  auto owner = dagflow::detail::make_owned_array<Object>(3);
  CHECK(Object::alive == 3 && owner[2].value == 0);
  CHECK(reinterpret_cast<std::uintptr_t>(owner.get()) % alignof(Object) == 0);
  std::thread consumer(
      [objects = std::move(owner)] { CHECK(objects[1].value == 0); });
  consumer.join();
  CHECK(!owner && Object::alive == 0);
  bool caught = false;
  ArrayElement::budget = 2;
  try {
    (void)dagflow::detail::make_owned_array<ArrayElement>(4);
  } catch (const std::runtime_error&) {
    caught = true;
  }
  CHECK(caught && ArrayElement::alive == 0);
  caught = false;
  try {
    (void)dagflow::detail::make_owned_array<Object>(SIZE_MAX);
  } catch (const std::length_error&) {
    caught = true;
  }
  CHECK(caught && Object::alive == 0);
}

void small_class_alignment_and_remote_free() {
  struct Block {
    void* pointer;
    std::size_t bytes, alignment;
  };
  std::vector<Block> blocks;
  std::thread producer([&] {
    for (std::size_t bytes :
         {1, 8, 16, 24, 31, 64, 96, 128, 176, 256, 512, 1024, 1025, 2048})
      for (std::size_t alignment : {1, 2, 4, 8, 16, 64, 256})
        for (int i = 0; i < 32; ++i) {
          auto* pointer = dagflow::detail::allocate_bytes(bytes, alignment);
          CHECK(reinterpret_cast<std::uintptr_t>(pointer) % alignment == 0);
          std::memset(pointer, 0x5a, bytes);
          blocks.push_back({pointer, bytes, alignment});
        }
  });
  producer.join();  // Originating allocator thread has exited.
  std::thread consumer([&] {
    for (auto block : blocks) {
      const auto* bytes = static_cast<const unsigned char*>(block.pointer);
      CHECK(bytes[0] == 0x5a && bytes[block.bytes - 1] == 0x5a);
      dagflow::detail::deallocate_bytes(block.pointer, block.alignment);
    }
  });
  consumer.join();
}

void zero_size_storage() {
  for (std::size_t alignment : {1, 2, 8, 16, 64, 256, 4096}) {
    auto* storage = dagflow::detail::allocate_bytes(0, alignment);
    CHECK(storage != nullptr);
    CHECK(reinterpret_cast<std::uintptr_t>(storage) % alignment == 0);
    dagflow::detail::deallocate_bytes(storage, alignment);
  }
  dagflow::detail::RuntimeAllocator<Object> allocator;
  auto* empty = allocator.allocate(0);
  CHECK(empty != nullptr);
  CHECK(reinterpret_cast<std::uintptr_t>(empty) % alignof(Object) == 0);
  allocator.deallocate(empty, 0);
  CHECK(Object::alive == 0);
}

int main() {
  zero_size_storage();
  // STL buffers use the real selected backend, including cross-thread free
  // after the allocating thread has exited and over-aligned element storage.
  {
    std::vector<Object, dagflow::detail::RuntimeAllocator<Object>> values;
    std::thread producer([&] { values = decltype(values)(3); });
    producer.join();
    CHECK(Object::alive == 3);
    CHECK(reinterpret_cast<std::uintptr_t>(values.data()) % alignof(Object) == 0);
    std::thread consumer([owned = std::move(values)] {
      CHECK(owned.size() == 3 && owned.back().value == 0);
    });
    consumer.join();
    CHECK(Object::alive == 0);
  }
  small_class_alignment_and_remote_free();
  array_storage();
  // Allocation origin may exit before another thread destroys its object.
  dagflow::detail::OwnedObject<Object> owner;
  std::thread producer(
      [&] { owner = dagflow::detail::make_owned<Object>(42); });
  producer.join();
  CHECK(Object::alive == 1);
  CHECK(reinterpret_cast<std::uintptr_t>(owner.get()) % alignof(Object) == 0);
  std::thread consumer(
      [object = std::move(owner)] { CHECK(object->value == 42); });
  consumer.join();
  CHECK(!owner && Object::alive == 0);

  struct Throws {
    Throws() { throw std::runtime_error("constructor failed"); }
  };
  bool caught = false;
  try {
    (void)dagflow::detail::make_owned<Throws>();
  } catch (const std::runtime_error&) {
    caught = true;
  }
  CHECK(caught);
}
