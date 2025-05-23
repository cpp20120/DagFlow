#include <chrono>
#include <cmath>
#include <iostream>
#include <vector>

#include "../include/thread_pool.hpp"

void calculateFactorial(int n) {
  long long result = 1;
  for (int i = 1; i <= n; ++i) {
	result *= i;
  }
  std::cout << "Factorial of " << n << " is " << result << std::endl;
}

void calculateFibonacci(int n) {
  int a = 0, b = 1, c;
  for (int i = 2; i <= n; ++i) {
	c = a + b;
	a = b;
	b = c;
  }
  std::cout << "Fibonacci number at position " << n << " is " << b << std::endl;
}

void simulateProjectileMotion(double initialVelocity, double angle,
							  double time) {
  double g = 9.81;
  double velocityX = initialVelocity * cos(angle);
  double velocityY = initialVelocity * sin(angle);
  double x = velocityX * time;
  double y = velocityY * time - 0.5 * g * time * time;
  std::cout << "Projectile position after " << time << " seconds: (" << x
			<< ", " << y << ")" << std::endl;
}

int main() {
  thread_pool::thread_pool pool;

  pool.submit([]() { calculateFactorial(10); });
  pool.submit([]() { calculateFibonacci(10); });
  pool.submit([]() { simulateProjectileMotion(50.0, 45.0, 2.0); });

  pool.wait();

  return 0;
}
