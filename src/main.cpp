#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "../include/api.hpp"

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
  double sum = std::accumulate(times.begin(), times.end(), 0.0);
  double mean = sum / times.size();
  auto [min_it, max_it] = std::minmax_element(times.begin(), times.end());

  std::cout << name << ":\n"
			<< "  Runs: " << times.size() << "\n"
			<< "  Mean: " << mean << " s\n"
			<< "  Min:  " << *min_it << " s\n"
			<< "  Max:  " << *max_it << " s\n";
}

void test_dependent_tasks(tp::Pool& pool, int chain_length) {
  tp::TaskScope scope(pool);
  std::vector<double> results(chain_length);

  auto first = scope.submit([&] { results[0] = 1.0; });

  tp::JobHandle prev = first;
  for (int i = 1; i < chain_length; ++i) {
	prev = scope.then(prev,
					  [&, i] { results[i] = results[i - 1] + std::sqrt(i); });
  }

  Timer timer;
  scope.run_and_wait();
  double time = timer.elapsed();

  std::cout << "Dependent chain (" << chain_length << " tasks): " << time
			<< " s, result=" << results.back() << "\n";
}

void test_independent_tasks(tp::Pool& pool, int task_count) {
  tp::TaskScope scope(pool);
  std::vector<double> results(task_count);
  std::vector<tp::JobHandle> handles;

  for (int i = 0; i < task_count; ++i) {
	handles.push_back(scope.submit([&, i] {
	  double x = 0;
	  for (int j = 0; j < 1000000; j++) {
		x += std::sin(i + j) * std::cos(i - j);
	  }
	  results[i] = x;
	}));
  }

  Timer timer;
  scope.run_and_wait();
  double time = timer.elapsed();

  std::cout << "Independent tasks (" << task_count << "): " << time << " s\n";
}

void test_parallel_for(tp::Pool& pool, int data_size) {
  std::vector<int> data(data_size);
  std::iota(data.begin(), data.end(), 0);

  struct Config {
	std::string name;
	tp::ScheduleOptions options;
  };

  std::vector<Config> configs = {
	  {"Default", {}},
	  {"High priority", {.priority = tp::Priority::High}},
	  {"Limited concurrency", {.concurrency = 2}},
	  {"Small chunks", {.concurrency = 0, .capacity = 100}}};

  for (auto& cfg : configs) {
	tp::TaskScope scope(pool);

	scope.parallel_for(
		data.begin(), data.end(),
		[](int& x) {
		  x = x * x - x;
		  for (int i = 0; i < 100; i++) {
			x = (x * 1103515245 + 12345) & 0x7fffffff;
		  }
		},
		cfg.options);

	Timer timer;
	scope.run_and_wait();
	double time = timer.elapsed();

	std::cout << "Parallel_for " << cfg.name << " (" << data_size
			  << " elements): " << time << " s\n";
  }
}

void test_workflow(tp::Pool& pool, int width, int depth) {
  tp::TaskScope scope(pool);
  std::vector<std::vector<double>> matrix(depth, std::vector<double>(width));

  std::vector<tp::JobHandle> prev_layer;
  for (int i = 0; i < width; ++i) {
	prev_layer.push_back(scope.submit([&, i] { matrix[0][i] = std::sin(i); }));
  }

  for (int d = 1; d < depth; ++d) {
	std::vector<tp::JobHandle> current_layer;
	for (int i = 0; i < width; ++i) {
	  auto h = scope.when_all(prev_layer, [&, d, i] {
		double sum = 0;
		for (double x : matrix[d - 1]) sum += x;
		matrix[d][i] = sum / width + d * i;
	  });
	  current_layer.push_back(h);
	}
	prev_layer = std::move(current_layer);
  }

  auto final = scope.when_all(prev_layer, [&] {
	double total = 0;
	for (auto& row : matrix) {
	  for (double x : row) total += x;
	}
	std::cout << "Workflow result: " << total << "\n";
  });

  Timer timer;
  scope.run_and_wait();
  double time = timer.elapsed();

  std::cout << "Workflow (width=" << width << ", depth=" << depth
			<< "): " << time << " s\n";
}

void test_error_handling(tp::Pool& pool) {
  tp::TaskScope scope(pool);

  auto good_task = scope.submit([] { std::cout << "Good task executed\n"; });

  auto bad_task =
	  scope.submit([] { throw std::runtime_error("Intentional error"); });

  auto dependent_task =
	  scope.then(good_task, [] { std::cout << "This should run\n"; });

  auto never_run =
	  scope.then(bad_task, [] { std::cout << "This should NOT run\n"; });

  try {
	scope.run_and_wait();
  } catch (const std::exception& e) {
	std::cout << "Caught exception: " << e.what() << "\n";
  }
}

void test_independent_tasks_batched(tp::Pool& pool, int task_count,
									int batch_size = 10) {
  tp::TaskScope scope(pool);
  std::vector<double> results(task_count);
  std::vector<tp::JobHandle> handles;

  for (int i = 0; i < task_count; i += batch_size) {
	int end = std::min(i + batch_size, task_count);
	handles.push_back(scope.submit([&results, i, end] {
	  for (int j = i; j < end; ++j) {
		double x = 0;
		for (int k = 0; k < 1000000; k++) {
		  x += std::sin(j + k) * std::cos(j - k);
		}
		results[j] = x;
	  }
	}));
  }

  Timer timer;
  scope.run_and_wait();
  std::cout << "Batched independent tasks (" << task_count
			<< "): " << timer.elapsed() << " s\n";
}

int main() {
  tp::Pool pool;
  const int runs = 5;

  std::vector<double> dep_times;
  for (int i = 0; i < runs; ++i) {
	Timer timer;
	test_dependent_tasks(pool, 1000);
	dep_times.push_back(timer.elapsed());
  }
  print_stats("Dependent tasks benchmark", dep_times);

  std::vector<double> indep_times;
  for (int i = 0; i < runs; ++i) {
	Timer timer;
	test_independent_tasks(pool, 1000);
	indep_times.push_back(timer.elapsed());
  }
  print_stats("Independent tasks benchmark", indep_times);

  std::vector<double> pf_times;
  for (int i = 0; i < runs; ++i) {
	Timer timer;
	test_parallel_for(pool, 1000000);
	pf_times.push_back(timer.elapsed());
  }
  print_stats("Parallel_for benchmark", pf_times);

  std::vector<double> workflow_times;
  for (int i = 0; i < runs; ++i) {
	Timer timer;
	test_workflow(pool, 10, 5);
	workflow_times.push_back(timer.elapsed());
  }
  print_stats("Workflow benchmark", workflow_times);

  std::vector<double> batch_times;
  for (int i = 0; i < runs; ++i) {
	Timer timer;
	test_independent_tasks_batched(pool, 1000, 10);
	batch_times.push_back(timer.elapsed());
  }
  print_stats("Batch benchmark", batch_times);

  test_error_handling(pool);

  return 0;
}