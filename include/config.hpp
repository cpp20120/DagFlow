#pragma once
#include <cstddef>

constexpr int CACHE_LINE_SIZE = 64;
/// Default inline-storage size (bytes) for task-body small_function instances.
constexpr int DAGFLOW_TASK_FN_SIZE = 128;
/// Capacity (in slots) of each central-shard ring-buffer queue. Must be a
/// power of two. 16 384 slots × 2 queues × N shards ≈ 4 MB for 16 threads.
constexpr std::size_t DAGFLOW_CENTRAL_QUEUE_CAPACITY = 1U << 14U;
