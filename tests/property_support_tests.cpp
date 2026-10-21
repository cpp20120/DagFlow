#include <array>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

#include "property_support.hpp"
#include "support.hpp"

namespace {
namespace property = dagflow::test::property;
using property::Command;

// The failure is reachable only through a create -> arm -> commit protocol.
// Deletion shrink candidates that remove the prerequisites must be rejected,
// not silently repaired or interpreted as a different command sequence.
struct ProtocolMachine {
  enum Op { create, arm, commit, noop };
  enum class State { empty, created, armed, committed };
  State state = State::empty;
  bool precondition(const Command& command) const {
    if (command.op == create) return state == State::empty;
    if (command.op == arm) return state == State::created && command.a > 0;
    if (command.op == commit) return state == State::armed;
    return command.op == noop;
  }
  void apply(const Command& command) {
    if (command.op == create) state = State::created;
    if (command.op == arm) state = State::armed;
    if (command.op == commit) state = State::committed;
  }
  void invariant() const {
    if (state == State::committed) throw std::runtime_error("protocol failure");
  }
};

// Removing select or shrinking its argument changes the failure identity.
// Such candidates must not be accepted just because they still fail.
struct SignatureMachine {
  bool selected = false;
  bool precondition(const Command& command) const { return command.op <= 2; }
  void apply(const Command& command) {
    if (command.op == 0) selected = command.a != 0;
    if (command.op == 1)
      throw std::runtime_error(selected ? "original failure" : "other failure");
  }
  void invariant() const {}
};

struct InitiallyBrokenMachine {
  bool precondition(const Command&) const { return true; }
  void apply(const Command&) {}
  void invariant() const { throw std::runtime_error("initial failure"); }
};

struct FinishMachine {
  bool finished = false;
  bool precondition(const Command&) const { return true; }
  void apply(const Command&) {}
  void finish() { finished = true; }
  void invariant() const {
    if (finished) throw std::runtime_error("finish invariant failure");
  }
};

void codec() {
  const std::vector<Command> commands{
      {0, 0, 0},
      {std::numeric_limits<unsigned>::max(), UINT64_MAX, UINT64_MAX},
      {7, 42, 99}};
  auto decoded = property::decode(property::encode(commands));
  CHECK(decoded.size() == commands.size());
  for (unsigned i = 0; i < commands.size(); ++i) {
    CHECK(decoded[i].op == commands[i].op);
    CHECK(decoded[i].a == commands[i].a);
    CHECK(decoded[i].b == commands[i].b);
  }
  CHECK(property::encode({}) == "-");
  CHECK(property::decode("-").empty());
  for (const char* malformed :
       {"", "0,1,2;", "0", "0,1", "0,1,", "0,1,2,3", "-1,0,0", "4294967296,0,0",
        "0,18446744073709551616,0", "0,1,2;;0,1,2", "0,1,x"}) {
    bool rejected = false;
    try {
      (void)property::decode(malformed);
    } catch (const std::invalid_argument&) {
      rejected = true;
    }
    CHECK(rejected);
  }
}

void generator() {
  property::Random random(0);
  constexpr std::array<std::uint64_t, 3> splitmix64_reference{
      0xe220a8397b1dcdafULL, 0x6e789e6aa1b965f4ULL, 0x06c45d188009454fULL};
  for (auto value : splitmix64_reference)
    CHECK(random.bounded(UINT64_MAX) == value);
  property::Random a(12345), b(12345);
  for (unsigned bound = 1; bound < 1000; ++bound) {
    const auto value = a.bounded(bound);
    CHECK(value < bound);
    CHECK(value == b.bounded(bound));
  }
  bool rejected = false;
  try {
    (void)random.bounded(0);
  } catch (const std::logic_error&) {
    rejected = true;
  }
  CHECK(rejected);
}

void protocol_replay_and_shrink() {
  const auto finalized = property::replay<FinishMachine>({});
  CHECK(finalized.valid && finalized.failure == "finish invariant failure");
  auto initial = property::replay<InitiallyBrokenMachine>({});
  CHECK(initial.valid && initial.failure == "initial failure");
  const auto initial_shrink = property::shrink<InitiallyBrokenMachine>(
      {{0, 1, 2}, {1, 3, 4}}, initial.failure);
  CHECK(initial_shrink.empty());

  auto valid = property::replay<ProtocolMachine>(
      {{ProtocolMachine::create, 0, 0}, {ProtocolMachine::arm, 1, 0}});
  CHECK(valid.valid && valid.failure.empty());
  auto invalid = property::replay<ProtocolMachine>(
      {{ProtocolMachine::arm, 1, 0}, {ProtocolMachine::commit, 0, 0}});
  CHECK(!invalid.valid && invalid.failure.empty());

  const std::vector<Command> trace{
      {ProtocolMachine::noop, 32, 64}, {ProtocolMachine::create, 7, 9},
      {ProtocolMachine::noop, 99, 99}, {ProtocolMachine::arm, 64, 32},
      {ProtocolMachine::noop, 11, 22}, {ProtocolMachine::commit, 12, 34}};
  const auto failure = property::replay<ProtocolMachine>(trace);
  CHECK(failure.valid && failure.failure == "protocol failure");
  const auto smaller =
      property::shrink<ProtocolMachine>(trace, failure.failure);
  CHECK(smaller.size() == 3);
  CHECK(smaller[0].op == ProtocolMachine::create);
  CHECK(smaller[1].op == ProtocolMachine::arm && smaller[1].a > 0);
  CHECK(smaller[1].a < 64);
  CHECK(smaller[2].op == ProtocolMachine::commit);
  for (const auto& command : smaller) CHECK(command.b == 0);
  const auto replayed = property::replay<ProtocolMachine>(smaller);
  CHECK(replayed.valid && replayed.failure == failure.failure);
}

void shrink_preserves_failure_identity() {
  const std::vector<Command> trace{{2, 8, 9}, {0, 64, 32}, {1, 77, 88}};
  const auto failure = property::replay<SignatureMachine>(trace);
  CHECK(failure.valid && failure.failure == "original failure");
  const auto smaller =
      property::shrink<SignatureMachine>(trace, failure.failure);
  CHECK(smaller.size() == 2);
  CHECK(smaller[0].op == 0 && smaller[0].a > 0);
  CHECK(smaller[1].op == 1);
  const auto replayed = property::replay<SignatureMachine>(smaller);
  CHECK(replayed.valid && replayed.failure == failure.failure);
  auto wrong_failure = property::replay<SignatureMachine>({{1, 0, 0}});
  CHECK(wrong_failure.valid && wrong_failure.failure == "other failure");
}
}  // namespace

int main() {
  codec();
  generator();
  protocol_replay_and_shrink();
  shrink_preserves_failure_identity();
}
