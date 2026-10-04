if(DAGFLOW_BUILD_EXAMPLES AND DAGFLOW_TARGET)
  dagflow_add_example(DagFlow_example SOURCES src/main.cpp LIBRARIES ${DAGFLOW_TARGET})
  dagflow_set_output_name(DagFlow_example dagflow-example)
  foreach(example IN ITEMS task_scope cancellation graph parallel_for batch)
    dagflow_add_example(DagFlow_example_${example} SOURCES examples/${example}.cpp LIBRARIES ${DAGFLOW_TARGET})
    dagflow_set_output_name(DagFlow_example_${example} dagflow-example-${example})
  endforeach()
endif()

