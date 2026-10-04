# Callable storage comparison

Measured on 2026-09-28 with Clang 22.1.8, `-std=c++23 -O3 -DNDEBUG`, on an
AMD Ryzen 7 6800H. This compares the previous three-thunk-pointer, inline-only
`small_function` against the static-ops-table implementation with SBO. Default
alignment is 16 bytes on this platform.

| Inline buffer | Previous wrapper size | Current wrapper size |
| --- | ---: | ---: |
| 64 bytes | 96 bytes | 80 bytes |
| 128 bytes | 160 bytes | 144 bytes |

The per-object reduction is 16 bytes. Each distinct target/configuration now has
a shared static table containing three function pointers. There is no separate
heap pointer in the wrapper; spilled objects use the inline buffer for that
pointer. The default constructor also avoids zeroing unused inline storage.

`bench/function_bench.cpp` uses four small target types, all eligible for inline
storage. Invocation runs through a non-inlined function over already type-erased
wrappers. The construction test includes allocation of two vectors, filling one,
moving each wrapper into the other, invoking and destroying them.

Five alternating before/after process pairs were run. Each process warms each
case and takes nine timing samples; the table reports the median of the five
process medians, in nanoseconds per operation.

| Case | Previous | Current |
| --- | ---: | ---: |
| Invoke, 256 wrappers × 8192 passes | 1.878 | 1.787 |
| Invoke, 65536 wrappers × 32 passes | 1.919 | 1.815 |
| Construct/move/invoke/destroy, 65536 wrappers | 46.421 | 44.039 |

These are small local improvements, approximately 5%, with visible timing noise.
For example, the construction case ranged from 44.106–58.463 ns before and
42.529–65.094 ns after. The runs were not CPU-pinned. They do not establish an
end-to-end scheduler/graph speedup or measure spill cost. The reliable benefit is
smaller wrapper storage plus support for oversized, over-aligned and throwing-move
targets. The ops table introduces an extra metadata indirection on invocation.

Build the current benchmark with:

```sh
cmake -S . -B /tmp/dagflow-function-bench -G Ninja \
  -DCMAKE_BUILD_TYPE=Release -DDAGFLOW_ALLOCATOR=system \
  -DDAGFLOW_BUILD_FUNCTION_BENCH=ON -DDAGFLOW_BUILD_EXAMPLES=OFF -DDAGFLOW_INSTALL=OFF
cmake --build /tmp/dagflow-function-bench --target dagflow_function_bench
/tmp/dagflow-function-bench/dagflow-function-bench
```

The comparison used standalone builds of the same source against a saved
pre-change header and the current header, without LTO. Invocation checksums
matched. Neither benchmark path instantiates a spilled target. Separate tests
record runtime allocation sizes/alignment, inject allocation/constructor failure,
and verify pointer-stealing moves and destruction with the selected backend.
