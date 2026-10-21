#include <array>
#include <atomic>
#include <cstdint>
#include <dagflow/detail/idle_accounting.hpp>
#include <optional>
#include <thread>

#include "property_support.hpp"

namespace {
using namespace dagflow::test::property;

class AccountingModel {
  enum Op {
    PublishWorker,
    PublishExternal,
    ProducerExit,
    Retire,
    StartWait,
    JoinWait,
    Notify,
    Drain,
    Recreate
  };
  // Six live identities exceed the four-entry TLS producer cache. Retirements
  // belong to executors, independently of publication lanes (IDLE-01..03).
  static constexpr unsigned Instances = 6, Workers = 3;
  struct State {
    unsigned pending{};
    std::uint64_t published{}, retired{};
  };
  std::array<State, Instances> model_{};
  std::array<std::optional<dagflow::detail::IdleAccounting>, Instances> sut_;
  std::array<std::atomic<unsigned>, Instances> pending_{};
  std::thread observer_;
  unsigned observed_{};
  std::atomic<bool> premature_{false};

  void retire(unsigned instance, unsigned worker) {
    --model_[instance].pending;
    ++model_[instance].retired;
    pending_[instance].fetch_sub(1, std::memory_order_release);
    sut_[instance]->retire(worker);
    sut_[instance]->worker_idle();
  }
  void drain(unsigned instance) {
    while (model_[instance].pending)
      retire(instance, model_[instance].pending % Workers);
    sut_[instance]->wait();
  }

 public:
  AccountingModel() {
    for (auto& instance : sut_) instance.emplace(Workers);
  }
  ~AccountingModel() {
    // An assertion/shrink rejection must still release the observer before
    // destroying borrowed accounting storage.
    for (unsigned instance = 0; instance < Instances; ++instance)
      drain(instance);
    if (observer_.joinable()) observer_.join();
  }
  Command next(Random& random) const {
    if (observer_.joinable() && model_[observed_].pending == 0)
      return {JoinWait, observed_, 0};
    auto instance = random.bounded(Instances);
    auto op = static_cast<unsigned>(random.bounded(9));
    if (op <= ProducerExit && model_[instance].pending >= 32) op = Retire;
    if (op == Retire && model_[instance].pending == 0) op = PublishWorker;
    if (op == StartWait &&
        (observer_.joinable() || model_[instance].pending == 0))
      op = Notify;
    if (op == JoinWait) {
      if (!observer_.joinable())
        op = Notify;
      else {
        instance = observed_;
        op = Drain;
      }
    }
    if (op == Recreate && (model_[instance].pending ||
                           (observer_.joinable() && instance == observed_)))
      op = Drain;
    return {op, instance, random.bounded(Workers)};
  }
  bool precondition(const Command& c) const {
    if (c.op > Recreate || c.a >= Instances || c.b >= Workers) return false;
    if (c.op <= ProducerExit)
      return model_[c.a].pending < 32 &&
             !(observer_.joinable() && c.a == observed_ &&
               model_[c.a].pending == 0);
    if (c.op == Retire) return model_[c.a].pending != 0;
    if (c.op == StartWait)
      return !observer_.joinable() && model_[c.a].pending != 0;
    if (c.op == JoinWait)
      return observer_.joinable() && c.a == observed_ &&
             model_[c.a].pending == 0;
    if (c.op == Recreate)
      return model_[c.a].pending == 0 &&
             !(observer_.joinable() && c.a == observed_);
    return true;
  }
  void apply(const Command& c) {
    auto& instance = *sut_[c.a];
    auto& state = model_[c.a];
    if (c.op <= ProducerExit) {
      // Publish before any waiter can observe this generation; active waiters
      // always have an older anchor obligation until JoinWait.
      pending_[c.a].fetch_add(1, std::memory_order_release);
      if (c.op == PublishWorker)
        instance.publish_worker(c.b);
      else if (c.op == PublishExternal)
        instance.publish_external();
      else {
        std::thread producer([&instance] { instance.publish_external(); });
        producer.join();
      }
      ++state.pending;
      ++state.published;
    } else if (c.op == Retire) {
      retire(c.a, c.b);
    } else if (c.op == StartWait) {
      observed_ = c.a;
      premature_.store(false);
      observer_ = std::thread([this, index = c.a] {
        sut_[index]->wait();
        if (pending_[index].load(std::memory_order_acquire) != 0)
          premature_.store(true, std::memory_order_release);
      });
    } else if (c.op == JoinWait) {
      observer_.join();
    } else if (c.op == Notify) {
      instance
          .worker_idle();  // A notification cannot forgive outstanding work.
    } else if (c.op == Drain) {
      drain(c.a);
    } else {
      instance.wait();
      sut_[c.a].emplace(
          Workers);  // Same address, distinct accounting identity.
      state = {};
      pending_[c.a].store(0);
    }
  }
  void invariant() {
    require(!premature_.load(std::memory_order_acquire),
            "wait cannot finish while a published obligation remains");
    for (unsigned i = 0; i < Instances; ++i) {
      require(model_[i].published - model_[i].retired == model_[i].pending,
              "reference ledger conserves publication obligations");
      if (model_[i].pending == 0) sut_[i]->wait();
    }
  }
  void finish() {
    for (unsigned i = 0; i < Instances; ++i) drain(i);
    if (observer_.joinable()) observer_.join();
  }
};
}  // namespace

int main() { run<AccountingModel>("idle_accounting", 48, 96); }
