#include <dagflow/build_info.hpp>

int main() {
  return dagflow::build_info::project != "DagFlowCapabilityCheck" ||
         dagflow::build_info::version != "1.0" ||
         dagflow::build_info::compiler.empty();
}
