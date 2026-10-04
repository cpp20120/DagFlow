#include <barrier>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <future>
#include <thread>
#include <vector>
#include "dagflow/thread_pool.hpp"
int main() {
  dagflow::Config cfg;
  cfg.threads = 8;
  cfg.shards = 1;
  cfg.pin_threads = false;
  cfg.idle_us_min = cfg.idle_us_max = 5'000'000;
  dagflow::Pool pool(cfg);
  for (int r = 0; r < 20; ++r) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    std::barrier together(8);
    auto body = [&] { together.arrive_and_wait(); };
    std::vector<decltype(body)> jobs(8, body);
    pool.submit_batch_detached(std::span{jobs});
    auto done = std::async(std::launch::async, [&] { pool.wait_idle(); });
    if (done.wait_for(std::chrono::seconds(1)) != std::future_status::ready) {
      std::fprintf(stderr, "wake relay stopped at round %d\n", r);
      std::quick_exit(2);
    }
    done.get();
  }
  std::puts("20 rounds passed");
}
