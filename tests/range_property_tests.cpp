#include <algorithm>
#include <atomic>
#include <dagflow/dagflow.hpp>
#include <deque>
#include <forward_list>
#include <memory>
#include <numeric>
#include <span>
#include <stdexcept>
#include <vector>

#include "property_support.hpp"

namespace {
using dagflow::test::property::Command;
using dagflow::test::property::Random;
using dagflow::test::property::require;

// Submitted -> observed is a real asynchronous transition. The independent
// model specifies visit multiplicity, error outcome and callable ownership,
// without reproducing the implementation's chunk/tile partition algorithm.
class RangeMachine {
  enum Op {
    resize,
    fixed,
    split,
    forward,
    batch,
    fail_fixed,
    fail_split,
    wait,
    observe,
    drop_observer
  };
  enum class Phase { idle, submitted, observed };
  struct Model {
    unsigned length = 65;
    Phase phase = Phase::idle;
    bool batch = false;
    bool failure = false;
    bool dropped = false;
    unsigned failing_element = 0;
  } model_;
  std::vector<unsigned> values_;
  std::deque<unsigned> segmented_;
  std::forward_list<unsigned> forward_;
  std::unique_ptr<std::atomic<unsigned>[]> visits_;
  std::weak_ptr<int> capture_;
  dagflow::Handle result_;
  std::vector<std::pair<dagflow::Handle, bool>> old_results_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};

  static void check_result(const dagflow::Handle& result, bool failure) {
    require(result.ready(), "observed range result stays ready");
    bool threw = false;
    try {
      result.rethrow_if_failed();
    } catch (const std::runtime_error&) {
      threw = true;
    }
    require(threw == failure, "result preserves the modeled body error");
  }
  void prepare() {
    values_.resize(model_.length);
    std::iota(values_.begin(), values_.end(), 0);
    segmented_.assign(values_.begin(), values_.end());
    forward_.assign(values_.begin(), values_.end());
    visits_ = std::make_unique<std::atomic<unsigned>[]>(model_.length);
  }

 public:
  // Dropping the last observer does not drain work. Keep borrowed arrays alive
  // through pool idle even when an invariant throws during trace shrinking.
  ~RangeMachine() { pool_.wait_idle(); }
  Command next(Random& random) const {
    if (model_.phase == Phase::submitted)
      return {model_.batch || model_.dropped || random.bounded(3)
                  ? wait
                  : drop_observer,
              0, 0};
    const auto op = static_cast<Op>(random.bounded(9));
    constexpr unsigned chunk = DAGFLOW_DEFAULT_RANGE_CHUNK;
    constexpr unsigned lengths[]{0,  1,   2,         63,    64,
                                 65, 129, chunk - 1, chunk, chunk + 1};
    if (op == resize)
      return {op, lengths[random.bounded(std::size(lengths))], 0};
    if (op == wait) return {observe, 0, 0};
    return {op, random.bounded(std::max(1u, model_.length)),
            random.bounded(17)};
  }
  bool precondition(const Command& command) const {
    if (command.op > drop_observer) return false;
    if (model_.phase == Phase::submitted)
      return command.op == wait ||
             (command.op == drop_observer && !model_.dropped && !model_.batch);
    if (command.op == wait || command.op == drop_observer) return false;
    if (command.op == resize)
      return command.a <= DAGFLOW_DEFAULT_RANGE_CHUNK + 1;
    if (command.op == fail_fixed || command.op == fail_split)
      return command.a < std::max(1u, model_.length);
    return true;
  }
  void apply(const Command& command) {
    if (command.op == resize) {
      model_.length = command.a;
      model_.phase = Phase::idle;
      return;
    }
    if (command.op == observe) return;
    if (command.op == drop_observer) {
      result_ = {};
      model_.dropped = true;
      return;
    }
    if (command.op == wait) {
      if (model_.batch || model_.dropped)
        pool_.wait_idle();
      else
        pool_.wait(result_);
      model_.phase = Phase::observed;
      if (!model_.batch && !model_.dropped) {
        check_result(result_, model_.failure);
        old_results_.emplace_back(result_, model_.failure);
      }
      return;
    }
    prepare();
    model_.batch = command.op == batch;
    model_.failure = model_.length != 0 &&
                     (command.op == fail_fixed || command.op == fail_split);
    model_.failing_element = command.a;
    model_.dropped = false;
    auto owner = std::make_shared<int>(42);
    capture_ = owner;
    auto invoke = [this, owner = std::move(owner)](unsigned index) {
      visits_[index].fetch_add(1);
      if (model_.failure && index == model_.failing_element)
        throw std::runtime_error("model range failure");
    };
    if (model_.batch) {
      // Cross both sides of the external 64-packet group boundary without
      // coupling the oracle to the grouping strategy.
      auto make_job = [&](unsigned i) { return [invoke, i] { invoke(i); }; };
      std::vector<decltype(make_job(0))> jobs;
      for (unsigned i = 0; i < model_.length; ++i) jobs.push_back(make_job(i));
      pool_.submit_batch_detached(std::span{jobs});
      result_ = {};
    } else if (command.op == forward) {
      result_ = pool_.for_each(forward_, std::move(invoke));
    } else if (command.op == split || command.op == fail_split) {
      // A deque exercises random-access splitting without contiguous storage.
      result_ = pool_.for_each_ws(segmented_, std::move(invoke), {}, command.b);
    } else {
      result_ = pool_.for_each(std::span{values_}, std::move(invoke));
    }
    if (!model_.batch)
      require(result_.valid() == (model_.length != 0),
              "empty range alone produces an invalid-ready result");
    model_.phase = Phase::submitted;
  }
  void finish() {
    if (model_.phase == Phase::submitted) apply({wait, 0, 0});
  }
  void invariant() const {
    if (model_.phase == Phase::observed) {
      require(capture_.expired(), "completion/idle releases operation capture");
      for (unsigned i = 0; i < model_.length; ++i) {
        const unsigned count = visits_[i].load();
        if (!model_.failure)
          require(count == 1, "successful operation visits every element once");
        else
          require(count <= 1, "failed range never invokes an element twice");
      }
      if (model_.failure)
        require(visits_[model_.failing_element].load() == 1,
                "the selected throwing element executes once");
    }
    for (const auto& [result, failure] : old_results_)
      check_result(result, failure);
  }
};

// A graph range retains its definition and borrowed iterators between runs,
// unlike a Pool range, whose callable must disappear at completion.
class GraphRangeMachine {
  enum Op { append, start, wait, clear, observe };
  struct Slice {
    unsigned offset;
    unsigned length;
  };
  struct Model {
    std::vector<Slice> slices;
    bool started = false;
    bool dirty = false;
    std::vector<unsigned> values;
  } model_;
  static constexpr unsigned extent = 2 * DAGFLOW_DEFAULT_RANGE_CHUNK + 1;
  std::vector<unsigned> values_ = std::vector<unsigned>(extent);
  std::vector<std::weak_ptr<int>> captures_;
  dagflow::JobHandle previous_;
  std::vector<dagflow::Handle> results_;
  dagflow::Pool pool_{dagflow::Config{.threads = 2}};
  dagflow::GraphScope scope_{pool_};

  void begin_run() {
    for (const auto& slice : model_.slices)
      for (unsigned i = slice.offset; i < slice.offset + slice.length; ++i)
        ++model_.values[i];
    model_.started = true;
    model_.dirty = false;
  }

 public:
  GraphRangeMachine() { model_.values.resize(extent); }
  Command next(Random& random) const {
    if (model_.started) return {random.bounded(3) == 0 ? start : wait, 0, 0};
    auto op = static_cast<Op>(random.bounded(5));
    if (op == append && model_.slices.size() == 4) op = wait;
    if (op != append) return {op, 0, 0};
    constexpr unsigned chunk = DAGFLOW_DEFAULT_RANGE_CHUNK;
    constexpr unsigned lengths[]{0, 1, chunk - 1, chunk, chunk + 1, extent};
    const unsigned length = lengths[random.bounded(std::size(lengths))];
    return {op, random.bounded(extent - length + 1), length};
  }
  bool precondition(const Command& command) const {
    if (command.op > observe) return false;
    if (model_.started) return command.op == wait || command.op == start;
    if (command.op == append)
      return model_.slices.size() < 4 && command.a <= extent &&
             command.b <= extent - command.a;
    return true;
  }
  void apply(const Command& command) {
    if (command.op == append) {
      auto owner = std::make_shared<int>(42);
      captures_.push_back(owner);
      auto work = [owner = std::move(owner)](unsigned& value) { ++value; };
      auto range = std::span{values_}.subspan(command.a, command.b);
      dagflow::ScheduleOptions options;
      options.concurrency = 4;
      if (model_.slices.empty())
        previous_ = scope_.parallel_for(range, work, options);
      else
        previous_ = scope_.parallel_for_after(previous_, range, work, options);
      model_.slices.push_back(
          {static_cast<unsigned>(command.a), static_cast<unsigned>(command.b)});
      model_.dirty = true;
    } else if (command.op == start) {
      if (!model_.started) begin_run();
      results_.push_back(scope_.run());
    } else if (command.op == wait) {
      if (!model_.started && model_.dirty) begin_run();
      scope_.wait();
      model_.started = false;
    } else if (command.op == clear) {
      scope_.clear();
      for (const auto& capture : captures_)
        require(capture.expired(), "clear releases persistent range callables");
      captures_.clear();
      model_.slices.clear();
      model_.dirty = false;
    }
  }
  void finish() {
    if (model_.started || model_.dirty) apply({wait, 0, 0});
    invariant();
    apply({clear, 0, 0});
  }
  void invariant() const {
    require(scope_.size() == model_.slices.size(), "one graph node per range");
    if (!model_.started) {
      require(values_ == model_.values,
              "dependency chain applies each range once per modeled run");
      for (const auto& result : results_) {
        require(result.ready(), "old range graph results remain ready");
        result.rethrow_if_failed();
      }
    }
    for (unsigned i = 0; i < captures_.size(); ++i)
      require(captures_[i].expired() == (model_.slices[i].length == 0),
              "nonempty graph ranges retain callable copies until clear");
  }
};
}  // namespace

int main() {
  dagflow::test::property::run<RangeMachine>("Pool range and batch lifecycle",
                                             24, 48);
  dagflow::test::property::run<GraphRangeMachine>(
      "GraphScope persistent range model", 16, 40);
}
