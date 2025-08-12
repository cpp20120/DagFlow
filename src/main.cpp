#include <algorithm>
#include <cmath>
#include <iostream>
#include <numeric>
#include <random>
#include <vector>

#include "../include/api.hpp"

// Тестовая ODE: y' = -lambda*y, y(0) = 1
static double euler_final(double lambda, double T, int steps) {
  const double h = T / steps;
  double y = 1.0;
  for (int i = 0; i < steps; ++i) y += h * (-lambda * y);
  return y;
}

static double heun_final(double lambda, double T, int steps) {
  const double h = T / steps;
  double y = 1.0;
  for (int i = 0; i < steps; ++i) {
	const double k1 = -lambda * y;
	const double y_pred = y + h * k1;
	const double k2 = -lambda * y_pred;
	y += h * 0.5 * (k1 + k2);
  }
  return y;
}

static double rk4_final(double lambda, double T, int steps) {
  const double h = T / steps;
  double y = 1.0;
  for (int i = 0; i < steps; ++i) {
	const double k1 = -lambda * y;
	const double k2 = -lambda * (y + 0.5 * h * k1);
	const double k3 = -lambda * (y + 0.5 * h * k2);
	const double k4 = -lambda * (y + h * k3);
	y += (h / 6.0) * (k1 + 2 * k2 + 2 * k3 + k4);
  }
  return y;
}

int main() {
  tp::Pool pool;

  const double T = 5.0;
  const int steps = 200;
  const int N = 20000;

  std::vector<int> idx(N);
  std::vector<double> lambdas(N);
  std::vector<double> err_euler(N), err_heun(N), err_rk4(N);

  tp::TaskScope scope(pool);

  // Источник данных
  auto gen = scope.submit(
	  [&] {
		std::iota(idx.begin(), idx.end(), 0);
		std::mt19937 rng{12345};
		std::uniform_real_distribution<double> dist(0.1, 5.0);
		for (int i = 0; i < N; ++i) lambdas[i] = dist(rng);
	  },
	  {.priority = tp::Priority::High});

  tp::ScheduleOptions heavy{.priority = tp::Priority::Normal,
							.affinity = std::nullopt,
							.concurrency = 2,
							.capacity = SIZE_MAX,
							.overflow = tp::TaskGraph::Overflow::Block};

  // Параллельные этапы, зависящие от gen:
  auto euler = scope.parallel_for_after(
	  gen, idx.begin(), idx.end(),
	  [&](int i) {
		const double lam = lambdas[i];
		const double yT = euler_final(lam, T, steps);
		const double yx = std::exp(-lam * T);
		err_euler[i] = std::abs(yT - yx);
	  },
	  heavy);

  auto heun = scope.parallel_for_after(
	  gen, idx.begin(), idx.end(),
	  [&](int i) {
		const double lam = lambdas[i];
		const double yT = heun_final(lam, T, steps);
		const double yx = std::exp(-lam * T);
		err_heun[i] = std::abs(yT - yx);
	  },
	  heavy);

  auto rk4 = scope.parallel_for_after(
	  gen, idx.begin(), idx.end(),
	  [&](int i) {
		const double lam = lambdas[i];
		const double yT = rk4_final(lam, T, steps);
		const double yx = std::exp(-lam * T);
		err_rk4[i] = std::abs(yT - yx);
	  },
	  heavy);

  // Агрегация результатов
  auto report = scope.when_all({euler, heun, rk4}, [&] {
	auto summarize = [](const std::vector<double>& v) {
	  const double sum = std::accumulate(v.begin(), v.end(), 0.0);
	  const double mean = sum / v.size();
	  const double mx = *std::max_element(v.begin(), v.end());
	  return std::pair<double, double>{mean, mx};
	};
	auto [me, xe] = summarize(err_euler);
	auto [mh, xh] = summarize(err_heun);
	auto [mr, xr] = summarize(err_rk4);

	std::cout.setf(std::ios::scientific);
	std::cout << "ODE y'=-lambda*y, y(0)=1, T=" << T << ", steps=" << steps
			  << ", N=" << N << "\n";
	std::cout << "Euler: mean=" << me << "  max=" << xe << "\n";
	std::cout << "Heun:  mean=" << mh << "  max=" << xh << "\n";
	std::cout << "RK4:   mean=" << mr << "  max=" << xr << "\n";
  });

  try {
	scope.run_and_wait();
  } catch (const std::exception& e) {
	std::cerr << "GRAPH ERROR: " << e.what() << "\n";
	return 1;
  }

  return 0;
}
