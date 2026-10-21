#include <algorithm>
#include <cstdint>
#include <dagflow/detail/scheduler.hpp>
#include <memory>
#include <optional>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::detail::Scheduler;
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;
using dagflow::test::property::run;

// Configuration is a transaction: constructing an invalid topology must not
// replace the previous immutable scheduler. The reference membership model is
// derived from a mathematical partition, not the scheduler's CSR arrays.
class TopologyMachine {
  enum Op {
    Workers,
    Shards,
    Batch,
    Mapping,
    Build,
    Affinity,
    RoundRobin,
    Destroy,
    Count
  };
  struct Config {
    unsigned workers = 1, shards = 1, batch = 1;
    std::vector<uint32_t> placement;
  } draft_, active_;
  std::unique_ptr<Scheduler> scheduler_;
  std::vector<uint32_t> homes_;
  unsigned next_round_robin_ = 0;

  static unsigned workers(const Config& cfg) {
    return std::max(1u, cfg.workers);
  }
  static unsigned shards(const Config& cfg) {
    return cfg.shards ? cfg.shards : workers(cfg);
  }
  static bool valid(const Config& cfg) {
    if (cfg.placement.empty()) return true;
    if (cfg.placement.size() != workers(cfg)) return false;
    for (auto id : cfg.placement)
      if (id >= shards(cfg)) return false;
    return true;
  }

 public:
  Command next(Random& random) const {
    Command c{static_cast<unsigned>(random.bounded(Count)), random.bounded(9),
              random.bounded(37)};
    if (!scheduler_ && (c.op == Affinity || c.op == RoundRobin)) c.op = Build;
    return c;
  }
  bool precondition(const Command& c) const {
    if (c.op >= Count || c.a >= 9 || c.b >= 37) return false;
    return (c.op != Affinity && c.op != RoundRobin) || bool(scheduler_);
  }
  void apply(const Command& c) {
    switch (c.op) {
      case Workers:
        draft_.workers = static_cast<unsigned>(c.a);
        break;
      case Shards:
        draft_.shards = static_cast<unsigned>(c.a);
        break;
      case Batch:
        draft_.batch = static_cast<unsigned>(c.b);
        break;
      case Mapping: {
        const auto count = workers(draft_);
        const auto groups = shards(draft_);
        draft_.placement.clear();
        switch (c.a % 5) {
          case 0:
            break;  // Balanced default.
          case 1:
            for (unsigned i = 0; i < count; ++i)
              draft_.placement.push_back((3 * i + c.b) % groups);
            break;
          case 2:
            draft_.placement.assign(count, c.b % groups);
            break;
          case 3:
            draft_.placement.assign(count + 1, 0);  // Wrong shape.
            break;
          case 4:
            draft_.placement.assign(count, 0);
            draft_.placement.back() = groups;  // Out-of-range shard ID.
            break;
        }
        break;
      }
      case Build: {
        std::unique_ptr<Scheduler> candidate;
        bool rejected = false;
        try {
          candidate = std::make_unique<Scheduler>(
              draft_.workers, draft_.shards, draft_.batch, draft_.placement);
        } catch (const std::invalid_argument&) {
          rejected = true;
        }
        require(rejected == !valid(draft_),
                "invalid explicit topology is rejected, valid is accepted");
        if (candidate) {
          scheduler_ = std::move(candidate);
          active_ = draft_;
          homes_.clear();
          for (unsigned i = 0; i < workers(active_); ++i)
            homes_.push_back(active_.placement.empty()
                                 ? uint64_t{i} * shards(active_) /
                                       workers(active_)
                                 : active_.placement[i]);
          next_round_robin_ = 0;
        }
        break;
      }
      case Affinity: {
        const auto hint = static_cast<uint32_t>(c.b * 37 + c.a);
        require(scheduler_->select_shard(hint) ==
                    homes_[hint % workers(active_)],
                "affinity always maps through the normalized worker index");
        break;
      }
      case RoundRobin:
        require(scheduler_->select_shard(std::nullopt) ==
                    next_round_robin_ % shards(active_),
                "unbound shard selection advances cyclically");
        ++next_round_robin_;
        break;
      case Destroy:
        scheduler_.reset();
        homes_.clear();
        next_round_robin_ = 0;
        break;
    }
  }
  void invariant() const {
    if (!scheduler_) return;
    const auto count = workers(active_);
    const auto groups = shards(active_);
    require(scheduler_->worker_count() == count,
            "zero workers normalize to one");
    require(scheduler_->shard_count() == groups,
            "zero shards normalize to one shard per worker");
    std::vector<unsigned> seen(count);
    unsigned total = 0;
    for (unsigned shard = 0; shard < groups; ++shard) {
      unsigned previous = 0;
      bool first = true;
      for (auto worker : scheduler_->members(shard)) {
        require(worker < count && homes_[worker] == shard,
                "every listed worker belongs to its modeled home shard");
        require(first || previous < worker,
                "membership is in stable worker-index order");
        first = false;
        previous = worker;
        ++seen[worker];
        ++total;
      }
    }
    require(total == count, "CSR membership partitions the worker domain");
    for (unsigned worker = 0; worker < count; ++worker) {
      require(seen[worker] == 1, "each worker appears exactly once");
      require(scheduler_->home_shard(worker) == homes_[worker],
              "worker home shard is independent of next routing decisions");
    }
  }
};
}  // namespace

int main() {
  run<TopologyMachine>("topology configuration and membership transactions",
                       80, 100);
}
