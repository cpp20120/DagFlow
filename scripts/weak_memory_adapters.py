"""Source-derived Relacy adapters; all omissions are bounded fixture choices."""
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]


def once(source, before, after):
    if source.count(before) != 1:
        raise ValueError(f"Expected unique source anchor: {before[:100]!r}")
    return source.replace(before, after, 1)


def definition(source, signature):
    """Copy one balanced definition, ignoring braces inside comments/strings."""
    if source.count(signature) != 1:
        raise ValueError(f"Expected unique definition: {signature}")
    start = source.index(signature)
    opening = source.index("{", start)
    tokens = re.compile(r'//[^\n]*|/\*.*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|[{}]', re.S)
    depth = 0
    for match in tokens.finditer(source, opening):
        token = match.group()
        if token == "{":
            depth += 1
        elif token == "}":
            depth -= 1
            if depth == 0:
                return source[start:match.end()]
    raise ValueError(f"Unbalanced definition: {signature}")


def atomics(source):
    source = source.replace("std::atomic<", "ModelAtomic<")
    source = source.replace("std::memory_order_", "rl::memory_order_")
    source = source.replace("std::atomic_thread_fence", "rl::atomic_thread_fence")
    source = source.replace("std::mutex", "rl::mutex")
    source = source.replace("std::lock_guard lock", "rl::lock_guard<rl::mutex> lock")
    source = source.replace("std::unique_lock lock", "rl::unique_lock<rl::mutex> lock")
    return re.sub(r"\bassert\(", "RL_ASSERT(", source)


def parking():
    source = (ROOT / "src/parking_lot.cpp").read_text()
    header = (ROOT / "include/dagflow/detail/parking_lot.hpp").read_text()
    waiter = definition(header, "struct Waiter {").replace("WaitCondition", "UntimedCV")
    domain = definition(header, "struct Domain {")
    prefix = '''#include "parking_fixture.hpp"
class ParkingLot {
 public:
WAITER;
DOMAIN;
  std::array<uint32_t, 1> members_{0};
  OneEntry<Waiter> waiters_;
  OneEntry<Domain> domains_;
  OneEntry<std::atomic<uint64_t>> idle_;
  ParkingLot() {
    waiters_[0].word_index = 0;
    waiters_[0].bit = 1;
    domains_[0] = {0, 1, 1, 1};
  }
  uint64_t prepare(uint32_t) noexcept;
  void cancel(uint32_t) noexcept;
  void wait(uint32_t, uint64_t, uint32_t, const std::atomic<bool>&);
  void signal(uint32_t, bool);
  void wake_one(uint32_t);
  void wake_worker(uint32_t);
  bool wake_word(uint32_t, uint64_t);
  bool wake_domain(uint32_t);
};
'''.replace("WAITER", waiter).replace("DOMAIN", domain)
    methods = ["uint64_t ParkingLot::prepare(", "void ParkingLot::cancel(",
               "void ParkingLot::wait(", "void ParkingLot::signal(uint32_t worker, bool claimed)",
               "void ParkingLot::wake_one(", "void ParkingLot::wake_worker(",
               "bool ParkingLot::wake_word(", "bool ParkingLot::wake_domain("]
    bodies = [definition(source, signature) for signature in methods]
    # The non-hook signal signature is followed by #endif before its body.
    bodies = [body.replace("\n#endif\n  auto& waiter", "\n  auto& waiter")
              if body.startswith("void ParkingLot::signal(") else body
              for body in bodies]
    return atomics(prefix + "\n".join(bodies))


def completion():
    source = (ROOT / "src/thread_pool.cpp").read_text()
    header = (ROOT / "include/dagflow/handle.hpp").read_text()
    credit = definition(header, "class CompletionCredit {")
    # Stack fixtures supply an already-owned credit; allocation is out of scope.
    credit = credit.replace(" private:", " public:")
    state = definition(header, "struct CompletionState {")
    state = once(state, "static void retire(CompletionState* state) noexcept;",
                 "static void retire(CompletionState* state) noexcept;\n"
                 "  static void model_finish_terminal(CompletionState* state) noexcept;")
    methods = ["void detail::CompletionState::retain(",
               "void detail::CompletionState::release(",
               "void detail::CompletionState::set_error(",
               "std::exception_ptr detail::CompletionState::get_error(",
               "void detail::CompletionState::add_dependent(",
               "void detail::CompletionState::retire(",
               "void detail::CompletionState::wait(",
               "void detail::CompletionCredit::fail(",
               "void detail::CompletionCredit::finish("]
    bodies = "\n".join(definition(source, signature) for signature in methods)
    # Split at the terminal worklist without changing any atomic operation.
    # This also permits an edge-arrival fixture to start at the two source
    # edges, avoiding enumeration of both already-checked source epilogues.
    retire = definition(source, "void detail::CompletionState::retire(")
    offset = retire.index("  CompletionState* pending = state;")
    replacement = (retire[:offset] + "  model_finish_terminal(state);\n}\n"
                   "void detail::CompletionState::model_finish_terminal(CompletionState* state) noexcept {\n"
                   + retire[offset:])
    bodies = once(bodies, retire, replacement)
    edge = definition(retire, "for (auto& edge : edges)")
    edge_body = edge[edge.index("{") + 1:-1]
    bodies += ("\nvoid model_deliver_edge(CompletionCredit edge) {\n"
               "  CompletionState* pending = nullptr;\n"
               "  int failure = 0;\n" + edge_body +
               "\n  if (pending) CompletionState::model_finish_terminal(pending);\n}\n")
    ready = definition(header, "[[nodiscard]] bool ready() const noexcept")
    ready = once(ready, "[[nodiscard]] bool ready() const noexcept",
                 "bool model_ready(const CompletionState* state_) noexcept")
    bodies += "\n" + ready
    bodies = bodies.replace("detail::CompletionState", "CompletionState")
    bodies = bodies.replace("detail::CompletionCredit", "CompletionCredit")
    bodies = re.sub(r"\berror\b", "error($)", bodies)
    bodies = bodies.replace("int failure;", "int failure{};")
    # Use an instrumented integer error identity instead of exception_ptr's
    # allocator/refcount internals. All first-error/registration logic is copied.
    result = '''#include "model_support.hpp"
#include <memory>
#include <utility>
#include <vector>
#include <dagflow/detail/fuzz_points.hpp>
class Handle;
struct CompletionState;
struct CompletionWaiter {
  struct Parking { void wake_worker(unsigned) { RL_ASSERT(false); } } parking;
  unsigned worker{};
  CompletionWaiter* next{};
};
template<class T> using RuntimeAllocator = std::allocator<T>;
template<class T> struct ObjectDeleter { void operator()(T* state) const; };
''' + credit + ";\n" + state + ";\n" + '''
inline std::ostream& operator<<(std::ostream& out, CompletionState::Dependents gate) {
  return out << unsigned(gate);
}
template<class T> void ObjectDeleter<T>::operator()(T* state) const {
  RL_ASSERT(state->credits.load(rl::memory_order_relaxed) == 0);
  RL_ASSERT(state->ready.load(rl::memory_order_relaxed));
  RL_ASSERT(!state->payload && state->dependents.empty() && !state->worker_waiters);
  // Storage remains pinned in the fixture; deallocation/reclamation is not modeled.
}
inline CompletionState::~CompletionState() = default;
''' + bodies
    result = result.replace("std::exception_ptr", "int")
    result = result.replace("int failure;", "int failure{};")
    result = once(result, "int error;", "rl::var<int> error;")
    # Credit::fail's parameter is also named error; keep it an ordinary value.
    result = result.replace("int error($)", "int error")
    result = result.replace("state_->set_error(std::move(error($)))", "state_->set_error(std::move(error))")
    return atomics(result)


def idle_accounting():
    source = (ROOT / "src/idle_accounting.cpp").read_text()
    header = (ROOT / "include/dagflow/detail/idle_accounting.hpp").read_text()
    state = definition(header, "class IdleAccounting {")
    state = once(state, " private:", " public:")
    state = once(state, "explicit IdleAccounting(uint32_t workers);",
                 "explicit IdleAccounting(uint32_t workers) : identity_(1), workers_(workers) {}")
    state = once(state, "~IdleAccounting();", "~IdleAccounting() = default;")
    # Fixed storage and an already-registered producer. The fast cache-hit path
    # supplies a lane without synchronization in the native implementation too.
    state = once(state, "Lane& producer_lane();",
                 "Lane& producer_lane() { RL_ASSERT(producers_); return producers_->lane; }")
    methods = ["void IdleAccounting::publication_exhausted(",
               "void IdleAccounting::publish_external(",
               "bool IdleAccounting::idle_locked(",
               "void IdleAccounting::worker_idle(", "void IdleAccounting::wait("]
    result = '''#include "idle_accounting_fixture.hpp"
''' + definition(source, "struct Total {") + ";\n" + state + ";\n"
    result += "\n".join(definition(source, signature) for signature in methods)
    return atomics(result).replace("std::condition_variable", "rl::condition_variable")


def shutdown():
    source = (ROOT / "src/thread_pool.cpp").read_text()
    header = (ROOT / "include/dagflow/thread_pool.hpp").read_text()
    guard = definition(header, "class PublicationGuard {")
    # Copy the actual admission/stop declarations, including initial values.
    start = header.index("  static constexpr uint64_t admission_closed_bit")
    end = header.index("\n};", start)
    fields = header[start:end]
    prefix = '''
#include "shutdown_fixture.hpp"
class Pool {
 public:
GUARD;
FIELDS
  IdleAccounting* accounting_;
  ModelParking* parking_;
  std::array<ModelThread, 0> threads_;
  // All modeled callers of guard/shutdown are external. Workers publish
  // descendants directly, under their live parent accounting obligation.
  static constexpr Pool* tls_pool_ = nullptr;
  Pool(IdleAccounting& accounting, ModelParking& parking)
      : accounting_(&accounting), parking_(&parking) {}
  void close() noexcept;
  bool closed() const noexcept;
  void enter_publication();
  void leave_publication() noexcept;
  void shutdown();
  void wait_idle();
};
'''.replace("GUARD", guard).replace("FIELDS", fields)
    methods = ["void Pool::close(", "bool Pool::closed(",
               "Pool::PublicationGuard::PublicationGuard(",
               "Pool::PublicationGuard::~PublicationGuard(",
               "void Pool::enter_publication(", "void Pool::leave_publication(",
               "void Pool::shutdown(", "void Pool::wait_idle("]
    return idle_accounting() + atomics(prefix + "\n".join(
        definition(source, signature) for signature in methods))
