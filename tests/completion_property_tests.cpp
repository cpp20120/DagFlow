#include <array>
#include <cstdint>
#include <dagflow/thread_pool.hpp>
#include <memory>
#include <utility>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using Credit = dagflow::detail::CompletionCredit;

// Sources are independent logical operations. A combined observer denotes a
// set of sources, irrespective of the shape or multiplicity of its join DAG.
class CompletionMachine {
  enum Op {
    Create,
    Fork,
    MoveCredit,
    Finish,
    Fail,
    Observe,
    Copy,
    Move,
    Drop,
    Combine,
    EmptyCombine,
    Count
  };
  struct Observer {
    bool valid = false;
    uint64_t sources = 0;
  };
  struct Source {
    int first_error = 0;
    unsigned destroyed = 0;
  };
  struct Payload {
    unsigned* destroyed;
    ~Payload() { ++*destroyed; }
  };
  static constexpr unsigned slots = 12;
  std::array<Source, 32> sources_{};
  unsigned source_count_ = 0;
  std::array<int, slots> owners_;
  std::array<Observer, slots> observers_{};
  std::array<Credit, slots> credits_{};
  std::array<dagflow::Handle, slots> handles_{};
  dagflow::Pool pool_{{.threads = 1}};

 public:
  CompletionMachine() { owners_.fill(-1); }

  void finish() {
    for (unsigned i = 0; i < slots; ++i) {
      credits_[i].finish();
      owners_[i] = -1;
    }
    invariant();
  }

  Command next(Random& random) const {
    for (;;) {
      Command command{static_cast<unsigned>(random.bounded(Count)),
                      random.bounded(slots), random.bounded(slots)};
      if (precondition(command)) return command;
    }
  }

  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= slots || c.b >= slots) return false;
    switch (c.op) {
      case Create:
        return owners_[c.a] < 0 && source_count_ < sources_.size();
      case Fork:
        return owners_[c.a] >= 0 && owners_[c.b] < 0;
      case Fail:
      case Observe:
        return owners_[c.a] >= 0;
      default:
        return true;
    }
  }

  void apply(const Command& c) {
    switch (c.op) {
      case Create: {
        const auto id = source_count_++;
        credits_[c.a] =
            Credit::create(std::make_unique<Payload>(&sources_[id].destroyed));
        owners_[c.a] = static_cast<int>(id);
        break;
      }
      case Fork:
        credits_[c.b] = credits_[c.a].fork();
        owners_[c.b] = owners_[c.a];
        break;
      case MoveCredit:
        credits_[c.b] = std::move(credits_[c.a]);
        if (c.a != c.b) owners_[c.b] = std::exchange(owners_[c.a], -1);
        break;
      case Finish:
        credits_[c.a].finish();
        owners_[c.a] = -1;
        break;
      case Fail: {
        const int error = static_cast<int>(c.b) + 1;
        credits_[c.a].fail(std::make_exception_ptr(error));
        auto& first = sources_[owners_[c.a]].first_error;
        if (!first) first = error;
        break;
      }
      case Observe:
        handles_[c.b] = credits_[c.a].handle();
        observers_[c.b] = {true, uint64_t{1} << owners_[c.a]};
        break;
      case Copy:
        handles_[c.b] = handles_[c.a];
        observers_[c.b] = observers_[c.a];
        break;
      case Move:
        handles_[c.b] = std::move(handles_[c.a]);
        if (c.a != c.b)
          observers_[c.b] = std::exchange(observers_[c.a], Observer{});
        break;
      case Drop:
        handles_[c.a] = {};
        observers_[c.a] = {};
        break;
      case Combine:
        handles_[c.a] = pool_.combine({handles_[c.a], handles_[c.b]});
        observers_[c.a] = {true,
                           observers_[c.a].sources | observers_[c.b].sources};
        break;
      case EmptyCombine:
        handles_[c.a] = pool_.combine({});
        observers_[c.a] = {};
        break;
    }
  }

  void invariant() const {
    uint64_t live = 0;
    for (unsigned i = 0; i < slots; ++i) {
      require(bool(credits_[i]) == (owners_[i] >= 0),
              "credit custody matches the model after move/finish");
      if (owners_[i] >= 0) live |= uint64_t{1} << owners_[i];
    }
    for (unsigned i = 0; i < source_count_; ++i)
      require(sources_[i].destroyed == ((live & (uint64_t{1} << i)) ? 0u : 1u),
              "only final credit destroys payload, exactly once");
    for (unsigned i = 0; i < slots; ++i) {
      const auto& model = observers_[i];
      require(handles_[i].valid() == model.valid, "observer validity");
      const bool ready = !(model.sources & live);
      require(handles_[i].ready() == ready,
              "join readiness is conjunction of source completion");
      if (!ready) continue;
      bool failed = false;
      bool allowed_error = false;
      int observed_error = 0;
      try {
        handles_[i].rethrow_if_failed();
      } catch (int error) {
        observed_error = error;
      }
      for (unsigned j = 0; j < source_count_; ++j) {
        if (!(model.sources & (uint64_t{1} << j))) continue;
        failed |= sources_[j].first_error != 0;
        allowed_error |= sources_[j].first_error == observed_error;
      }
      require((observed_error != 0) == failed,
              "join preserves failure iff a source failed");
      require(!observed_error || allowed_error,
              "propagated error is a retained first source error");
    }
  }
};
}  // namespace

int main() {
  dagflow::test::property::run<CompletionMachine>("completion", 96, 128);
}
