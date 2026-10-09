# Training and merge are explicit framework targets, never part of ALL.
function(dagflow_register_training target)
  if(TARGET ${target})
    boilerplate_pgo_workload(${target} ${ARGN})
  endif()
endfunction()
dagflow_register_training(dagflow_runtime_suite --workers 2 --tasks 512
  --iterations 64 --repeats 2 --warmup 1)
dagflow_register_training(dagflow_runtime_suite --workers 2 --tasks 512
  --iterations 64 --repeats 2 --warmup 1 --latency)
dagflow_register_training(dagflow_runtime_bench 2 512 2)
dagflow_register_training(dagflow_stress_bench --workers 2 --tasks 512
  --iterations 64 --repeats 2 --warmup 1 --warmup-ms 0 --bursts 2 --idle-us 100)
dagflow_register_training(dagflow_example)
foreach(example IN ITEMS task_scope cancellation graph parallel_for batch)
  dagflow_register_training(dagflow_example_${example})
endforeach()
dagflow_register_training(dagflow_public_api_bench --benchmark chain
  --workers 2 --runs 2 --warmup 1 --payload-rounds 8)
dagflow_register_training(dagflow_tbb_bench --benchmark chain
  --workers 2 --runs 2 --warmup 1 --payload-rounds 8)
dagflow_register_training(dagflow_function_bench)
