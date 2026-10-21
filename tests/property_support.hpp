#pragma once

// A small state-machine runner shared by ordinary CTest executables. Models
// describe contracts; command generation is independent of libFuzzer inputs.
#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <source_location>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace dagflow::test::property {
struct Command {
  unsigned op{};
  std::uint64_t a{}, b{};
};

class Random {
 public:
  explicit Random(std::uint64_t seed) : state_(seed) {}
  std::uint64_t bounded(std::uint64_t bound) {
    if (bound == 0) throw std::logic_error("empty command generator domain");
    // SplitMix64: a specified sequence, independent of STL distributions.
    auto value = (state_ += 0x9e3779b97f4a7c15ULL);
    value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9ULL;
    value = (value ^ (value >> 27)) * 0x94d049bb133111ebULL;
    return (value ^ (value >> 31)) % bound;
  }

 private:
  std::uint64_t state_;
};

inline void require(
    bool condition, const char* property,
    std::source_location where = std::source_location::current()) {
  if (!condition)
    throw std::runtime_error(std::string(where.file_name()) + ":" +
                             std::to_string(where.line()) + ": " + property);
}

inline std::uint64_t number(std::string_view value) {
  std::uint64_t result{};
  const auto [end, error] =
      std::from_chars(value.data(), value.data() + value.size(), result);
  if (error != std::errc{} || end != value.data() + value.size())
    throw std::invalid_argument("invalid property runner integer");
  return result;
}

inline std::uint64_t setting(const char* name, std::uint64_t fallback) {
  const auto* value = std::getenv(name);
  return value ? number(value) : fallback;
}

inline std::string encode(const std::vector<Command>& commands) {
  if (commands.empty()) return "-";
  std::string result;
  for (const auto& command : commands) {
    if (!result.empty()) result += ';';
    result += std::to_string(command.op) + ',' + std::to_string(command.a) +
              ',' + std::to_string(command.b);
  }
  return result;
}

inline std::vector<Command> decode(std::string_view input) {
  std::vector<Command> result;
  if (input == "-") return result;
  if (input.empty() || input.back() == ';')
    throw std::invalid_argument(
        "empty property command; use '-' for no commands");
  while (!input.empty()) {
    const auto end = input.find(';');
    auto item = input.substr(0, end);
    const auto first = item.find(',');
    const auto second = item.find(',', first == item.npos ? 0 : first + 1);
    if (first == item.npos || second == item.npos)
      throw std::invalid_argument("expected property trace op,a,b;op,a,b");
    const auto op = number(item.substr(0, first));
    if (op > UINT32_MAX)
      throw std::invalid_argument("property opcode overflow");
    result.push_back({static_cast<unsigned>(op),
                      number(item.substr(first + 1, second - first - 1)),
                      number(item.substr(second + 1))});
    if (end == input.npos) break;
    input.remove_prefix(end + 1);
  }
  return result;
}

struct Result {
  bool valid{true};
  std::string failure;
};

struct Selection {
  bool attempted{}, matched{};
  ~Selection() {
    if (attempted && !matched) {
      std::fprintf(stderr, "DAGFLOW_PROPERTY_MODEL matched no model\n");
      std::fflush(stderr);
      std::_Exit(EXIT_FAILURE);
    }
  }
};
inline Selection selection;

template <class Machine>
Result replay(const std::vector<Command>& commands) {
  try {
    // Heap storage also accommodates large bounded-queue fixtures on Windows.
    auto machine = std::make_unique<Machine>();
    machine->invariant();
    for (const auto& command : commands) {
      if (!machine->precondition(command)) return {false, {}};
      machine->apply(command);
      machine->invariant();
    }
    if constexpr (requires { machine->finish(); }) {
      machine->finish();
      machine->invariant();
    }
    return {};
  } catch (const std::exception& error) {
    return {true, error.what()};
  }
}

template <class Machine>
std::vector<Command> shrink(std::vector<Command> commands,
                            const std::string& failure) {
  unsigned budget = 256;
  auto preserves = [&](const std::vector<Command>& candidate) {
    if (budget == 0) return false;
    --budget;
    const auto result = replay<Machine>(candidate);
    return result.valid && result.failure == failure;
  };
  // Delete chunks first, preserving only valid traces with the same failure.
  for (auto width = commands.size(); width != 0 && budget; width /= 2) {
    for (std::size_t start = 0; start + width <= commands.size() && budget;) {
      auto candidate = commands;
      candidate.erase(candidate.begin() + start,
                      candidate.begin() + start + width);
      if (preserves(candidate))
        commands = std::move(candidate);
      else
        ++start;
    }
  }
  for (std::size_t i = 0; i < commands.size() && budget; ++i) {
    for (unsigned field = 0; field < 2 && budget; ++field) {
      const auto original = field == 0 ? commands[i].a : commands[i].b;
      for (auto smaller : {std::uint64_t{0}, original / 2}) {
        const auto current = field == 0 ? commands[i].a : commands[i].b;
        if (smaller >= current) continue;
        auto candidate = commands;
        (field == 0 ? candidate[i].a : candidate[i].b) = smaller;
        if (preserves(candidate)) commands = std::move(candidate);
      }
    }
  }
  return commands;
}

template <class Machine>
void run(const char* name, unsigned cases = 64, unsigned steps = 64) {
  selection.attempted = true;
  if (const auto* filter = std::getenv("DAGFLOW_PROPERTY_MODEL"))
    if (std::string_view(filter) != name) return;
  selection.matched = true;
  const auto seed = setting("DAGFLOW_PROPERTY_SEED", 0x444147464c4f57ULL);
  const auto case_count = setting("DAGFLOW_PROPERTY_CASES", cases);
  const auto step_count = setting("DAGFLOW_PROPERTY_STEPS", steps);
  require(case_count > 0 && case_count <= 100000, "bounded nonzero case count");
  require(step_count > 0 && step_count <= 10000, "bounded nonzero step count");
  const auto* explicit_trace = std::getenv("DAGFLOW_PROPERTY_TRACE");
  for (std::uint64_t index = 0; index < (explicit_trace ? 1 : case_count);
       ++index) {
    const auto case_seed = seed + index * 0x9e3779b97f4a7c15ULL;
    std::vector<Command> commands;
    Result result;
    if (explicit_trace) {
      commands = decode(explicit_trace);
      result = replay<Machine>(commands);
      require(result.valid, "replay trace satisfies command preconditions");
    } else {
      try {
        auto machine = std::make_unique<Machine>();
        Random random(case_seed);
        machine->invariant();
        for (std::uint64_t step = 0; step < step_count; ++step) {
          const auto command = machine->next(random);
          require(machine->precondition(command),
                  "generator satisfies command preconditions");
          commands.push_back(command);
          machine->apply(command);
          machine->invariant();
        }
        if constexpr (requires { machine->finish(); }) {
          machine->finish();
          machine->invariant();
        }
      } catch (const std::exception& error) {
        result.failure = error.what();
      }
    }
    if (!result.failure.empty()) {
      std::fprintf(stderr, "PROPERTY %s seed=%llu: %s\noriginal: %s\n", name,
                   static_cast<unsigned long long>(case_seed),
                   result.failure.c_str(), encode(commands).c_str());
      const auto repeated = replay<Machine>(commands);
      if (repeated.valid && repeated.failure == result.failure) {
        const auto minimal = shrink<Machine>(commands, result.failure);
        std::fprintf(stderr,
                     "Replay this executable with:\n"
                     "DAGFLOW_PROPERTY_MODEL='%s' "
                     "DAGFLOW_PROPERTY_TRACE='%s'\n",
                     name, encode(minimal).c_str());
      } else {
        std::fprintf(stderr, "Failure did not reproduce; trace not shrunk.\n");
      }
      throw std::runtime_error(std::string("property failed: ") + name);
    }
  }
  std::printf("property %s: %llu cases, %llu steps, seed=%llu%s\n", name,
              static_cast<unsigned long long>(explicit_trace ? 1 : case_count),
              static_cast<unsigned long long>(step_count),
              static_cast<unsigned long long>(seed),
              explicit_trace ? " (explicit replay)" : "");
}
}  // namespace dagflow::test::property
