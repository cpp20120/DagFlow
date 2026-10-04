#pragma once

#include "dagflow/detail/small_function.hpp"

namespace dagflow {

/// Strictly inline, move-only callable. Targets must fit StorageSize and
/// StorageAlign and be nothrow-movable/destructible; otherwise compilation
/// fails. Empty invocation is a precondition violation. No allocations are
/// performed by the wrapper itself.
template <class Sig, std::size_t StorageSize = DAGFLOW_DEFAULT_FUNCTION_STORAGE_SIZE,
          std::size_t StorageAlign = alignof(std::max_align_t)>
using inplace_function =
    detail::basic_function<Sig, StorageSize, StorageAlign, false>;

}  // namespace dagflow
