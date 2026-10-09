include_guard(GLOBAL)

function(_boilerplate_runtime_threads target)
  if(NOT TARGET Threads::Threads)
    find_package(Threads REQUIRED)
  endif()
  target_link_libraries(${target} PRIVATE Threads::Threads)
endfunction()

# Runtime archetypes intentionally encode opinionated local-performance policy.
# Use `runtime` for a portable baseline and layer individual primitives when an
# artifact has distribution/ABI constraints different from these profiles.
boilerplate_define_policy(runtime
  INHERITS minimal
  HOOKS _boilerplate_runtime_threads)

# Clang/Linux hardening composition. Windows callers compose runtime hardening
# windows-cfg explicitly; CFG is not a substitute for Clang's type checks.
boilerplate_define_policy(runtime-hardened
  INHERITS runtime hardening cfi)

# CPU-bound scheduler/executor style: whole-program optimization and ELF call
# overhead reductions are valuable; no plugin/interposition semantics assumed.
boilerplate_define_policy(runtime-dagflow
  INHERITS runtime native full-lto lld gc-sections no-plt no-semantic-interposition icf)

# Network/event-loop style: keep ThinLTO iteration times reasonable and avoid
# disabling semantic interposition by default because DSOs/plugins are common.
boilerplate_define_policy(runtime-webserver
  INHERITS runtime native thin-lto lld gc-sections no-plt icf)

# Diagnostic policies start from the portable runtime baseline instead of a
# tuned release policy, avoiding accidental sanitizer+PGO/LTO combinations.
boilerplate_define_policy(runtime-profiled
  INHERITS runtime debug-symbols frame-pointers)
boilerplate_define_policy(runtime-asan
  INHERITS runtime debug-symbols frame-pointers asan-ubsan)
boilerplate_define_policy(runtime-tsan
  INHERITS runtime debug-symbols frame-pointers tsan)
boilerplate_define_policy(runtime-dagflow-profiled
  INHERITS runtime-dagflow debug-symbols frame-pointers)
boilerplate_define_policy(runtime-webserver-profiled
  INHERITS runtime-webserver debug-symbols frame-pointers)
