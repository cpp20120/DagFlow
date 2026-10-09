#include <boilerplate/build_info.hpp>

int main() {
  return boilerplate::build_info::project != "DagFlowCapabilityCheck" ||
         boilerplate::build_info::version != "1.0" ||
         boilerplate::build_info::compiler.empty();
}
