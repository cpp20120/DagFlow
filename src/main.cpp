#include "../include/thread_pool.hpp"

int main() {
  thread_pool::thread_pool<int> pool(4);

  pool.set_on_task_complete_callback([](const thread_pool::TaskMetadata &meta) {
    std::cout << "Task " << meta.name << " completed successfully\n";
  });

  pool.set_on_task_error_callback(
      [](std::exception_ptr eptr, const thread_pool::TaskMetadata &meta) {
        try {
          std::rethrow_exception(eptr);
        } catch (const std::exception &e) {
          std::cerr << "Task " << meta.name << " failed: " << e.what() << "\n";
        }
      });

  auto [fA, hA] = pool.submit(1, []() {
    std::cout << "Executing task A\n";
    return 1;
  });
  auto [fB, hB] = pool.submit(1, []() {
    std::cout << "Executing task B\n";
    return 2;
  });

  auto [fC, hC] = pool.submit_with_dependencies(2, {hA}, []() {
    std::cout << "Executing task C\n";
    return 1 * 2;
  });
  auto [fD, hD] = pool.submit_with_dependencies(2, {hA, hB}, []() {
    std::cout << "Executing task D\n";
    return 1 + 2;
  });

  auto [fE, hE] = pool.submit_with_dependencies(3, {hC, hD}, []() {
    std::cout << "Executing task E\n";
    return 2 * 3;
  });

  try {
    pool.debug_check_state();
    std::cout << "Getting result for fA, valid: " << fA.valid() << std::endl;
    int resultA = fA.get();
    std::cout << "Result A: " << resultA << "\n";
    std::cout << "Getting result for fB, valid: " << fB.valid() << std::endl;
    int resultB = fB.get();
    std::cout << "Result B: " << resultB << "\n";
    std::cout << "Getting result for fC, valid: " << fC.valid() << std::endl;
    int resultC = fC.get();
    std::cout << "Result C: " << resultC << "\n";
    std::cout << "Getting result for fD, valid: " << fD.valid() << std::endl;
    int resultD = fD.get();
    std::cout << "Result D: " << resultD << "\n";
    std::cout << "Getting result for fE, valid: " << fE.valid() << std::endl;
    int finalResult = fE.get();
    std::cout << "Final computation: " << finalResult << "\n";
    pool.debug_check_state();
  } catch (const std::future_error &e) {
    std::cerr << "Future error: " << e.what() << std::endl;
  } catch (const std::exception &e) {
    std::cerr << "Exception: " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown error" << std::endl;
  }

  std::cout << "Waiting for all tasks to complete...\n";
  pool.wait_for_tasks();
  std::cout << "All tasks completed successfully\n";
  return 0;
}