#pragma once
#include <relacy/relacy.hpp>

template <class T>
struct ModelAtomic : rl::atomic<T> {
  using rl::atomic<T>::atomic;
  // Explicitly retain the C++20 initial value in Relacy's read history.
  ModelAtomic() { this->store(T{}, rl::memory_order_relaxed); }
  static constexpr bool is_always_lock_free = true;
};

template <class Model>
int check_model(const char* name) {
  rl::test_params params;
  params.search_type = rl::sched_full;
  params.execution_depth_limit = 200;
  if constexpr (requires { Model::context_bound; }) {
    params.search_type = rl::sched_bound;
    params.context_bound = Model::context_bound;
  }
  std::cout << "WEAK_MEMORY_CONFIG " << name << " search="
            << (params.search_type == rl::sched_full ? "sched_full"
                                                     : "sched_bound")
            << " context_bound="
            << (params.search_type == rl::sched_full ? 0 : params.context_bound)
            << " depth=" << params.execution_depth_limit << '\n';
  const bool passed = rl::simulate<Model>(params);
  std::cout << "WEAK_MEMORY_RESULT " << name << " "
            << rl::test_result_str(params.test_result)
            << " executions=" << params.stop_iteration << '\n';
  if (passed) return 0;
  if (params.test_result == rl::test_result_user_assert_failed ||
      params.test_result == rl::test_result_data_race)
    return 86;
  // A model deadlock is a finite counterexample, not a process timeout. Only
  // the explicitly selected lost-wake mutants accept this outcome.
  if (params.test_result == rl::test_result_deadlock) return 87;
  return 2;
}
