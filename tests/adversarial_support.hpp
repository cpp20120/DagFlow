#pragma once

#include <exception>
#include <dagflow/thread_pool.hpp>
#include "support.hpp"

namespace adversarial {
struct Failure { unsigned id; };

inline dagflow::Config config(unsigned workers = 1) {
  dagflow::Config cfg;
  cfg.threads = workers;
  cfg.pin_threads = false;
  return cfg;
}

inline unsigned failure_id(const dagflow::Handle& handle) {
  CHECK(handle.ready());
  try { handle.rethrow_if_failed(); }
  catch (const Failure& failure) { return failure.id; }
  CHECK(false);
  return 0;
}

inline std::exception_ptr error(const dagflow::Handle& handle) {
  CHECK(handle.ready());
  try { handle.rethrow_if_failed(); }
  catch (const Failure&) { return std::current_exception(); }
  return {};
}
} // namespace adversarial
