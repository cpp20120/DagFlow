# `detail/` PBT addendum for the 2026-10-21 working tree

This change is based on the uploaded `DagFlow(4).zip`, **not** on the older
snapshot used by `../.cache/DagFlow-detail-coverage-PBT.patch`. Do not apply both patches.

The current tree already contains:

- `detail_vector_boundary_property_tests.cpp`: saturating `small_vector`
  capacity arithmetic, aliasing and copy-constructor rollback;
- `detail_accounting_boundary_property_tests.cpp`: quiescent
  `IdleAccounting` histories around `UINT64_MAX` and the isolated
  `std::terminate` retirement case;
- `dagflow_detail_accounting_fault_boundary_property_tests` for the
  instrumented runtime.

This addendum therefore changes **no** runtime files, no fault hooks and no
CMake target registrations. It extends the existing vector-boundary executable
with the separate `RuntimeOwnershipModel`.

## Independent properties

| Operation | Check |
| --- | --- |
| Array/object construction and replacement | Live object count equals the independently tracked ownership ledger |
| Array/object move and reset | Exact transfer of ownership; destination's previous objects are retired |
| `unique_ptr::release` followed by direct deleter | Exactly-once destruction, no resource left behind |
| Null `ObjectDeleter` / `ArrayDeleter` | No effect on the current live-object ledger |
| Empty owned array / `RuntimeAllocator::allocate(0)` | Empty owner and reclaimable, correctly aligned zero-sized storage |
| Oversized array / allocator count | `length_error` / `bad_array_new_length` **before** allocating |
| Over-aligned objects (`alignas(128)`) | Returned addresses satisfy the actual element alignment |

Uses the shared deterministic PBT generator and replay/shrinker. It is a
runtime-memory contract check, not a claim of exhaustive concurrency checking
or 100% source branch coverage.

## Run

```bash
cmake --preset core
cmake --build --preset core --target dagflow_detail_vector_boundary_property_tests -j 2
ctest --preset core -R '^dagflow_detail_vector_boundary_property_tests$' --output-on-failure
ctest --preset core -L pbt --output-on-failure
```

If an earlier build was interrupted by `^C`, rerun the build: it is not a
compilation failure. Rebuilding all PBT targets is optional when only the
existing vector-boundary test source has changed.
