#pragma once
#include <cstddef>
#include <cstdint>

constexpr std::size_t DAGFLOW_CACHE_LINE_SIZE = 64;
// Compatibility spelling retained for existing low-level users.
constexpr std::size_t CACHE_LINE_SIZE = DAGFLOW_CACHE_LINE_SIZE;
/// Default inline-storage size (bytes) for task-body small_function instances.
constexpr int DAGFLOW_TASK_FN_SIZE = 128;
/// Default inline-storage size (bytes) for public callable wrappers.
constexpr std::size_t DAGFLOW_DEFAULT_FUNCTION_STORAGE_SIZE = 64;
/// Default inline element count for small_vector instances.
constexpr std::size_t DAGFLOW_DEFAULT_SMALL_VECTOR_CAPACITY = 4;
/// Fixed slots in each worker's local deque (one per priority).
#if defined(DAGFLOW_TEST_TINY_QUEUES)
constexpr std::size_t DAGFLOW_LOCAL_QUEUE_CAPACITY = 2;
constexpr std::size_t DAGFLOW_CENTRAL_QUEUE_CAPACITY = 2;
#else
constexpr std::size_t DAGFLOW_LOCAL_QUEUE_CAPACITY = 1U << 10U;
/// Capacity (in slots) of each central-shard ring-buffer queue. Must be a
/// power of two. 16 384 slots × 2 queues × N shards ≈ 4 MB for 16 threads.
constexpr std::size_t DAGFLOW_CENTRAL_QUEUE_CAPACITY = 1U << 14U;
#endif
/// Default target range size used by range helpers before splitting work.
constexpr std::size_t DAGFLOW_DEFAULT_RANGE_CHUNK = 1U << 14U;
/// A pool always has at least one worker, even when Config::threads is zero.
constexpr uint32_t DAGFLOW_MIN_WORKER_COUNT = 1;
/// Default central queue transfer batch size.
#if defined(DAGFLOW_TEST_TINY_QUEUES)
constexpr uint32_t DAGFLOW_DEFAULT_CENTRAL_BATCH = 1;
#else
constexpr uint32_t DAGFLOW_DEFAULT_CENTRAL_BATCH = 1024;
#endif
/// Default worker parking backoff bounds, in microseconds.
constexpr uint32_t DAGFLOW_DEFAULT_IDLE_US_MIN = 50;
constexpr uint32_t DAGFLOW_DEFAULT_IDLE_US_MAX = 200;
