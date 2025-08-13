/*/#include <tbb/blocked_range.h>
#include <tbb/global_control.h>
#include <tbb/parallel_for.h>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <vector>

using namespace std::chrono;

struct Timer {
  high_resolution_clock::time_point start;
  Timer() : start(high_resolution_clock::now()) {}
  double elapsed() const {
	return duration_cast<duration<double>>(high_resolution_clock::now() - start)
		.count();
  }
};

void print_stats(const std::string& name, const std::vector<double>& times) {
  if (times.empty()) return;
  double sum = std::accumulate(times.begin(), times.end(), 0.0);
  double mean = sum / times.size();
  auto [min_it, max_it] = std::minmax_element(times.begin(), times.end());

  std::cout << name << ":\n"
			<< "  Runs: " << times.size() << "\n"
			<< "  Mean: " << mean << " s\n"
			<< "  Min:  " << *min_it << " s\n"
			<< "  Max:  " << *max_it << " s\n";
}

void test_dependent_tasks_tbb(int chain_length) {
  std::vector<double> results(chain_length);

  tbb::task_group tg;
  results[0] = 1.0;

  for (int i = 1; i < chain_length; ++i) {
	tg.run([&, i] {
	  results[i] = results[i - 1] + std::sqrt(static_cast<double>(i));
	});
	tg.wait();
  }

  std::cout << "Dependent chain (" << chain_length
			<< " tasks): result=" << results.back() << "\n";
}

void test_independent_tasks_tbb(int task_count) {
  std::vector<double> results(task_count);
  tbb::task_group tg;

  for (int i = 0; i < task_count; ++i) {
	tg.run([&, i] {
	  double x = 0;
	  for (int j = 0; j < 1'000'000; j++) {
		x += std::sin(i + j) * std::cos(i - j);
	  }
	  results[i] = x;
	});
  }

  tg.wait();
}

void test_parallel_for_tbb(int data_size) {
  std::vector<int> data(data_size);
  std::iota(data.begin(), data.end(), 0);

  struct Config {
	std::string name;
	int grainsize;
  };

  std::vector<Config> configs = {
	  {"Default grainsize", 0},
	  {"Grainsize=1", 1},
	  {"Grainsize=4k", 4096},
	  {"Grainsize=16k", 1 << 14},
  };

  for (auto& cfg : configs) {
	std::vector<int> local = data;	// копия, чтобы каждый запуск было честно

	if (cfg.grainsize <= 0) {
	  tbb::parallel_for(tbb::blocked_range<int>(0, data_size),
						[&](const tbb::blocked_range<int>& r) {
						  for (int i = r.begin(); i != r.end(); ++i) {
							int x = local[i];
							x = x * x - x;
							for (int k = 0; k < 100; ++k) {
							  x = (x * 1103515245 + 12345) & 0x7fffffff;
							}
							local[i] = x;
						  }
						});
	} else {
	  tbb::parallel_for(tbb::blocked_range<int>(0, data_size, cfg.grainsize),
						[&](const tbb::blocked_range<int>& r) {
						  for (int i = r.begin(); i != r.end(); ++i) {
							int x = local[i];
							x = x * x - x;
							for (int k = 0; k < 100; ++k) {
							  x = (x * 1103515245 + 12345) & 0x7fffffff;
							}
							local[i] = x;
						  }
						});
	}

	std::cout << "Parallel_for TBB " << cfg.name << " (" << data_size
			  << " elems), sample=" << local[data_size / 2] << "\n";
  }
}

/**
 * Workflow (width x depth).
 */
/**
void test_workflow_tbb(int width, int depth) {
  std::vector<std::vector<double>> matrix(depth, std::vector<double>(width));

  for (int i = 0; i < width; ++i) {
	matrix[0][i] = std::sin(static_cast<double>(i));
  }

  for (int d = 1; d < depth; ++d) {
	tbb::task_group tg;
	for (int i = 0; i < width; ++i) {
	  tg.run([&, d, i] {
		double sum = 0;
		for (double x : matrix[d - 1]) sum += x;
		matrix[d][i] = sum / width + static_cast<double>(d * i);
	  });
	}
	tg.wait();
  }

  double total = 0;
  for (auto& row : matrix)
	for (double x : row) total += x;

  std::cout << "Workflow TBB (w=" << width << ", d=" << depth
			<< ") result=" << total << "\n";
}
   */
/**
 * Noop tasks: просто N пустых тасков.
 */
/**
void test_noop_tasks_tbb(int task_count) {
  tbb::task_group tg;
  for (int i = 0; i < task_count; ++i) {
	tg.run([] {});
  }
  tg.wait();
}

// Error handle
void test_error_handling_tbb() {
  tbb::task_group tg;

  tg.run([] { std::cout << "Good task executed\n"; });
  tg.run([] { throw std::runtime_error("Intentional TBB error"); });
  tg.run([] { std::cout << "This may or may not run before exception\n"; });

  try {
	tg.wait();
  } catch (const std::exception& e) {
	std::cout << "Caught exception (TBB): " << e.what() << "\n";
  }
}

int main() {
  const int runs = 5;

  // Dependent
  {
	std::vector<double> times;
	for (int i = 0; i < runs; ++i) {
	  Timer t;
	  test_dependent_tasks_tbb(1000);
	  times.push_back(t.elapsed());
	}
	print_stats("TBB: Dependent tasks benchmark", times);
  }

  // Independent
  {
	std::vector<double> times;
	for (int i = 0; i < runs; ++i) {
	  Timer t;
	  test_independent_tasks_tbb(1000);
	  times.push_back(t.elapsed());
	}
	print_stats("TBB: Independent tasks benchmark", times);
  }

  // parallel_for
  {
	std::vector<double> times;
	for (int i = 0; i < runs; ++i) {
	  Timer t;
	  test_parallel_for_tbb(1'000'000);
	  times.push_back(t.elapsed());
	}
	print_stats("TBB: Parallel_for benchmark", times);
  }

  // workflow
  {
	std::vector<double> times;
	for (int i = 0; i < runs; ++i) {
	  Timer t;
	  test_workflow_tbb(10, 5);
	  times.push_back(t.elapsed());
	}
	print_stats("TBB: Workflow benchmark", times);
  }

  // batched independent
  {
	std::vector<double> times;
	for (int r = 0; r < runs; ++r) {
	  Timer t;
	  int task_count = 1000;
	  int batch_size = 10;
	  tbb::task_group tg;
	  for (int i = 0; i < task_count; i += batch_size) {
		int lo = i;
		int hi = std::min(i + batch_size, task_count);
		tg.run([lo, hi] {
		  for (int j = lo; j < hi; ++j) {
			volatile double x = 0;
			for (int k = 0; k < 1'000'000; ++k) {
			  x += std::sin(j + k) * std::cos(j - k);
			}
		  }
		});
	  }
	  tg.wait();
	  times.push_back(t.elapsed());
	}
	print_stats("TBB: Batched independent tasks benchmark", times);
  }

  // noop
  {
	std::vector<double> times;
	for (int i = 0; i < runs; ++i) {
	  Timer t;
	  test_noop_tasks_tbb(1'000'000);
	  times.push_back(t.elapsed());
	}
	print_stats("TBB: Noop benchmark", times);
  }

  // error handling
  test_error_handling_tbb();

  return 0;
}
		*/				