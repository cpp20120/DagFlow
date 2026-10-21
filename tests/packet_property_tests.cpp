#include <array>
#include <cstdint>
#include <exception>
#include <dagflow/thread_pool.hpp>
#include <memory>
#include <stdexcept>
#include <utility>

#include "property_support.hpp"

namespace {
using dagflow::detail::CompletionCredit;
using dagflow::detail::OwnedScheduledTask;
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;

// A packet owns its callable, not its observer. Forked credits can keep the
// operation alive after packet/capture destruction; neither a moved packet nor
// a dropped observer may manufacture another invocation or payload release.
class PacketMachine {
  enum Op {
    Create,
    Invoke,
    Fail,
    MovePacket,
    DropPacket,
    Fork,
    RetireFork,
    MoveFork,
    DropObserver,
    Count
  };
  static constexpr unsigned slots = 8, sources = 48;
  struct Capture {
    unsigned* destroyed;
    ~Capture() { ++*destroyed; }
  };
  struct Callable {
    unsigned* calls;
    std::unique_ptr<Capture> capture;
    void operator()(CompletionCredit&) { ++*calls; }
  };
  struct Payload {
    unsigned* destroyed;
    ~Payload() { ++*destroyed; }
  };
  struct Source {
    unsigned calls{}, captures{}, payloads{};
    bool invoked{}, failed{};
    dagflow::Priority priority{dagflow::Priority::Normal};
    dagflow::Handle observer;
  };
  std::array<Source, sources> source_{};
  std::array<OwnedScheduledTask, slots> packets_{};
  std::array<CompletionCredit, slots> forks_{};
  std::array<int, slots> packet_owner_{}, fork_owner_{};
  unsigned created_ = 0;

 public:
  PacketMachine() {
    packet_owner_.fill(-1);
    fork_owner_.fill(-1);
  }

  Command next(Random& random) const {
    for (unsigned attempt = 0; attempt < 128; ++attempt) {
      Command c{static_cast<unsigned>(random.bounded(Count)),
                random.bounded(slots), random.bounded(slots)};
      if (precondition(c)) return c;
    }
    // Every reachable state permits retiring something or a no-op reset of an
    // observer; bounded fallback avoids an unbounded generator search.
    for (unsigned i = 0; i < slots; ++i) {
      if (packet_owner_[i] >= 0) return {DropPacket, i, 0};
      if (fork_owner_[i] >= 0) return {RetireFork, i, 0};
    }
    if (created_ < sources) return {Create, 0, 0};
    return {DropObserver, 0, 0};
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= slots || c.b >= slots) return false;
    switch (c.op) {
      case Create:
        return packet_owner_[c.a] < 0 && created_ < sources;
      case Invoke:
        return packet_owner_[c.a] >= 0 &&
               !source_[packet_owner_[c.a]].invoked;
      case Fail:
      case DropPacket:
        return packet_owner_[c.a] >= 0;
      case MovePacket:
        return packet_owner_[c.a] >= 0 && packet_owner_[c.b] < 0;
      case Fork:
        return packet_owner_[c.a] >= 0 && fork_owner_[c.b] < 0;
      case RetireFork:
        return fork_owner_[c.a] >= 0;
      case MoveFork:
        return fork_owner_[c.a] >= 0 && fork_owner_[c.b] < 0;
      case DropObserver:
        return true;
      default:
        return false;
    }
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Create: {
        const auto id = created_++;
        auto& source = source_[id];
        source.priority = (c.b & 1) ? dagflow::Priority::High
                                    : dagflow::Priority::Normal;
        auto credit = CompletionCredit::create(
            std::make_unique<Payload>(&source.payloads));
        source.observer = credit.handle();
        packets_[c.a] = dagflow::detail::make_scheduled_task(
            Callable{&source.calls,
                     std::make_unique<Capture>(&source.captures)},
            source.priority, std::move(credit));
        packet_owner_[c.a] = id;
        break;
      }
      case Invoke:
        packets_[c.a]->ops->invoke(packets_[c.a].get());
        source_[packet_owner_[c.a]].invoked = true;
        break;
      case Fail:
        packets_[c.a]->done.fail(std::make_exception_ptr(123));
        source_[packet_owner_[c.a]].failed = true;
        break;
      case MovePacket:
        packets_[c.b] = std::move(packets_[c.a]);
        packet_owner_[c.b] = std::exchange(packet_owner_[c.a], -1);
        break;
      case DropPacket:
        packets_[c.a].reset();
        packet_owner_[c.a] = -1;
        break;
      case Fork:
        forks_[c.b] = packets_[c.a]->done.fork();
        fork_owner_[c.b] = packet_owner_[c.a];
        break;
      case RetireFork:
        forks_[c.a].finish();
        fork_owner_[c.a] = -1;
        break;
      case MoveFork:
        forks_[c.b] = std::move(forks_[c.a]);
        fork_owner_[c.b] = std::exchange(fork_owner_[c.a], -1);
        break;
      case DropObserver:
        if (created_) source_[c.a % created_].observer = {};
        break;
    }
  }
  void invariant() const {
    std::array<unsigned, sources> credits{}, live_packets{};
    for (unsigned i = 0; i < slots; ++i) {
      require(bool(packets_[i]) == (packet_owner_[i] >= 0),
              "packet storage and custody agree");
      require(bool(forks_[i]) == (fork_owner_[i] >= 0),
              "forked completion custody agrees");
      if (packet_owner_[i] >= 0) {
        const auto id = packet_owner_[i];
        ++credits[id];
        ++live_packets[id];
        require(packets_[i]->prio == source_[id].priority,
                "packet priority survives custody transfer");
        require(packets_[i]->ops != nullptr &&
                    packets_[i]->ops->invoke != nullptr &&
                    packets_[i]->ops->destroy != nullptr,
                "constructed packet has live operations");
      }
      if (fork_owner_[i] >= 0) ++credits[fork_owner_[i]];
    }
    for (unsigned id = 0; id < created_; ++id) {
      const auto& source = source_[id];
      require(source.calls == unsigned(source.invoked),
              "packet invokes callable no more than once");
      require(source.captures == unsigned(live_packets[id] == 0),
              "capture is destroyed once on relinquishing packet storage");
      require(source.payloads == unsigned(credits[id] == 0),
              "final credit alone destroys operation payload");
      require(!source.observer.valid() ||
                  source.observer.ready() == (credits[id] == 0),
              "observer readiness follows credits, not packet storage");
      if (source.observer.valid() && credits[id] == 0) {
        bool failed = false;
        try {
          source.observer.rethrow_if_failed();
        } catch (int error) {
          require(error == 123, "retained packet failure identity");
          failed = true;
        }
        require(failed == source.failed,
                "terminal observer retains packet-side error");
      }
    }
  }
  void finish() {
    for (unsigned i = 0; i < slots; ++i) {
      packets_[i].reset();
      packet_owner_[i] = -1;
      forks_[i].finish();
      fork_owner_[i] = -1;
    }
    invariant();
  }
};
}  // namespace

int main() {
  run<PacketMachine>("packet storage, invocation and credit lifetime", 64,
                     112);
}
