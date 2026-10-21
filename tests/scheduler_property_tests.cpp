#include <array>
#include <cstdint>
#include <dagflow/detail/scheduler.hpp>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

// Custody is deliberately a set, not a copy of the scheduling algorithm.
// Priority/affinity do not promise a global dequeue order (SCH-01, SCH-07).
template <unsigned Workers, unsigned Shards>
class SchedulerModel {
  enum Op { Local, External, Overflow, Acquire, Drain, Affinity };
  struct Model {
    std::array<bool, 48> queued{};
    unsigned pending{};
  } model_;
  std::array<dagflow::detail::ScheduledTask, 48> packets_{};
  static std::array<std::uint32_t, Workers> placement() {
    std::array<std::uint32_t, Workers> result{};
    for (unsigned i = 0; i < Workers; ++i) result[i] = (3 * i + 1) % Shards;
    return result;
  }
  std::array<std::uint32_t, Workers> placement_ = placement();
  dagflow::detail::Scheduler scheduler_{Workers, Shards, 3, placement_};

  void acquire(unsigned worker) {
    auto* packet = scheduler_.try_acquire(worker);
    if (!packet) return;  // A bounded acquisition may miss remote work.
    unsigned index = 0;
    while (index < packets_.size() && packet != &packets_[index]) ++index;
    require(index < packets_.size(), "acquired packet belongs to fixture");
    require(model_.queued[index], "packet custody transfers exactly once");
    model_.queued[index] = false;
    --model_.pending;
  }

 public:
  Command next(Random& random) const {
    const auto op = static_cast<unsigned>(random.bounded(6));
    if (op <= Overflow) {
      if (model_.pending == packets_.size()) return {Drain, 0, 0};
      auto index = random.bounded(packets_.size());
      while (model_.queued[index]) index = (index + 1) % packets_.size();
      // The low bit chooses priority; the remaining bits choose the source.
      return {op, index, random.bounded(2 * (Workers + Shards))};
    }
    return {op, random.bounded(Workers), random.bounded(10000)};
  }
  bool precondition(const Command& c) const {
    if (c.op > Affinity) return false;
    if (c.op <= Overflow) return c.a < packets_.size() && !model_.queued[c.a];
    return c.a < Workers;
  }
  void apply(const Command& c) {
    if (c.op <= Overflow) {
      auto& packet = packets_[c.a];
      packet.prio =
          c.b % 2 ? dagflow::Priority::High : dagflow::Priority::Normal;
      bool accepted = true;
      if (c.op == Local)
        accepted = scheduler_.try_submit_local((c.b / 2) % Workers, &packet);
      else if (c.op == External)
        accepted = scheduler_.try_submit_external((c.b / 2) % Shards, &packet);
      else
        scheduler_.submit_overflow((c.b / 2) % Shards, &packet);
      if (model_.pending == 0)
        require(accepted, "empty scheduler accepts publication");
      if (accepted) {
        model_.queued[c.a] = true;
        ++model_.pending;
      }
    } else if (c.op == Acquire) {
      acquire(c.a);
    } else if (c.op == Drain) {
      // No producers run here. All workers probing must drain a finite set.
      for (unsigned round = 0; round < 256 && model_.pending; ++round)
        for (unsigned worker = 0; worker < Workers; ++worker) acquire(worker);
      require(model_.pending == 0, "quiescent scheduler drains accepted work");
      for (unsigned worker = 0; worker < Workers; ++worker) {
        require(!scheduler_.has_local_work(worker), "drained owner is empty");
        require(scheduler_.try_acquire(worker) == nullptr,
                "drained scheduler has no duplicate packets");
      }
    } else {
      const auto hint = static_cast<std::uint32_t>(c.b);
      require(scheduler_.select_shard(hint) == placement_[hint % Workers],
              "affinity selects the normalized worker's home shard");
      require(scheduler_.select_shard(std::nullopt) < Shards,
              "unbound affinity selects a valid shard");
    }
  }
  void invariant() const {
    std::array<unsigned, Workers> occurrences{};
    for (unsigned shard = 0; shard < Shards; ++shard)
      for (auto worker : scheduler_.members(shard)) {
        require(worker < Workers, "topology contains valid worker IDs");
        ++occurrences[worker];
        require(placement_[worker] == shard, "topology preserves placement");
      }
    for (unsigned worker = 0; worker < Workers; ++worker) {
      require(occurrences[worker] == 1, "worker belongs to exactly one shard");
      require(scheduler_.home_shard(worker) == placement_[worker],
              "home shard agrees with independent placement model");
      if (scheduler_.has_local_work(worker))
        require(model_.pending > 0, "local work has a live custody obligation");
    }
  }
  void finish() { apply({Drain, 0, 0}); }
};
}  // namespace

int main() {
  run<SchedulerModel<1, 1>>("scheduler_single");
  run<SchedulerModel<4, 7>>("scheduler_sparse");
  run<SchedulerModel<5, 2>>("scheduler_shared_domains");
}
