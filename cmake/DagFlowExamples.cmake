if(DAGFLOW_BUILD_EXAMPLES AND DAGFLOW_TARGET)
  boilerplate_add_example(dagflow_example SOURCES examples/basic.cpp LIBRARIES ${DAGFLOW_TARGET})
  boilerplate_set_output_name(dagflow_example dagflow-example)
  foreach(example IN ITEMS task_scope cancellation graph parallel_for batch)
    boilerplate_add_example(dagflow_example_${example} SOURCES examples/${example}.cpp LIBRARIES ${DAGFLOW_TARGET})
    boilerplate_set_output_name(dagflow_example_${example} dagflow-example-${example})
  endforeach()
endif()

