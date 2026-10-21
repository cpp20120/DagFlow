#pragma once
// Deliberately unavailable in production builds. The CMake test target builds
// a separate copy of the runtime with this flag; normal binaries never pay
// for injection checks or receive these mutable controls.
#if !defined(DAGFLOW_TEST_FAULT_INJECTION)
#error "test_faults.hpp must only be included by the isolated test runtime"
#endif
#include <cstddef>
#include <stdexcept>

namespace dagflow::detail::test_faults {
struct injected_wait_error final : std::runtime_error {
  injected_wait_error() : std::runtime_error("injected condition wait failure") {}
};
struct injected_publication_error final : std::runtime_error {
  injected_publication_error() : std::runtime_error("injected publication failure") {}
};
// Thread-local controls scope each fault to the initiating test thread, so
// background workers execute real code without inheriting a failure.
inline thread_local int fail_external_publication_at = -1;
// Deterministic queue-full simulation for the producer's retry protocol.
// This rejects *before* ownership transfers into ingress. It is scoped to
// the submitting test thread and does not mutate any queue internals.
inline thread_local unsigned reject_external_ingress = 0;
inline thread_local bool fail_idle_wait = false;
inline thread_local bool fail_parking_wait = false;
inline thread_local bool force_worker_retire_overflow = false;
inline thread_local bool force_worker_publish_overflow = false;
inline thread_local bool force_pool_admission_overflow = false;
inline thread_local bool force_scope_admission_overflow = false;
inline thread_local std::size_t graph_node_limit = 0;
inline thread_local std::size_t graph_edge_limit = 0;
inline thread_local bool bypass_graph_early_validation = false;

inline void reset() noexcept {
  fail_external_publication_at = -1;
  reject_external_ingress = 0;
  fail_idle_wait = fail_parking_wait = false;
  force_worker_retire_overflow = force_worker_publish_overflow = false;
  force_pool_admission_overflow = force_scope_admission_overflow = false;
  graph_node_limit = graph_edge_limit = 0;
  bypass_graph_early_validation = false;
}
struct ResetOnExit {
  ~ResetOnExit() { reset(); }
};
inline void external_publication() {
  if (fail_external_publication_at < 0) return;
  if (fail_external_publication_at-- == 0) {
    fail_external_publication_at = -1;
    throw injected_publication_error{};
  }
}
inline void idle_wait() {
  if (fail_idle_wait) {
    fail_idle_wait = false;
    throw injected_wait_error{};
  }
}
inline void parking_wait() {
  if (fail_parking_wait) {
    fail_parking_wait = false;
    throw injected_wait_error{};
  }
}
}  // namespace dagflow::detail::test_faults
