"""Bounded Relacy cases and explicit source mutations for each protocol."""
from weak_memory_adapters import ROOT, atomics, completion, definition, idle_accounting, once, parking, shutdown

FAILURE = ("USER ASSERT FAILED", "DATA RACE")
LOST_WAKE = (*FAILURE, "DEADLOCK")


def method(source, signature, before, after):
    body = definition(source, signature)
    return once(source, body, once(body, before, after))


def cases(protocol):
    """Yield (variant name, adapted source, [(suite, permitted results)])."""
    if protocol == "deque":
        base = atomics((ROOT / "include/dagflow/detail/chase_lev_deque.hpp").read_text())
        yield "baseline", base, [(s, ("SUCCESS",)) for s in
                                 ("publication", "slot-publication", "last-item", "two-items")]
        needle = ("DAGFLOW_FUZZ_POINT(deque_before_publish);\n"
                  "    bottom_.store(b + 1, rl::memory_order_release);")
        yield "deque-relax-publication", once(base, needle, needle.replace("release", "relaxed")), [
            ("publication", FAILURE), ("slot-publication", FAILURE)]
    elif protocol == "parking":
        base = parking()
        yield "baseline", base, [(s, ("SUCCESS",)) for s in ("announce", "sleep", "wake")]
        changed = method(base, "void ParkingLot::wake_one(",
                         "rl::atomic_thread_fence(rl::memory_order_seq_cst);", "/* no publisher fence */")
        yield "parking-drop-publisher-fence", changed, [("announce", FAILURE)]
        changed = base
        for before in (
                "waiter.sleeping.store(true, rl::memory_order_seq_cst)",
                "waiter.epoch.load(rl::memory_order_seq_cst) != epoch",
                "waiter.epoch.fetch_add(1, rl::memory_order_seq_cst)",
                "waiter.sleeping.load(rl::memory_order_seq_cst)"):
            changed = once(changed, before, before.replace("seq_cst", "relaxed"))
        yield "parking-relax-sleep-handshake", changed, [("sleep", LOST_WAKE)]
        changed = once(base, "  {\n    rl::lock_guard<rl::mutex> lock(waiter.mutex);\n  }",
                       "  /* no CV enrollment handshake */")
        yield "parking-drop-cv-handoff", changed, [("sleep", LOST_WAKE)]
        changed = once(base, "  waiter.cv.notify_one();", "  /* missing notify */")
        yield "parking-drop-notify", changed, [("sleep", LOST_WAKE)]
    elif protocol == "completion":
        base = completion()
        yield "baseline", base, [(s, ("SUCCESS",)) for s in
                                 ("aggregate", "ready", "wait", "registration", "registration-error", "cascade")]
        needle = "state->credits.fetch_sub(1, rl::memory_order_acq_rel)"
        yield "completion-retire-release-only", once(base, needle, needle.replace("acq_rel", "release")), [
            ("aggregate", FAILURE)]
        needle = "current->ready.store(true, rl::memory_order_release)"
        yield "completion-ready-relaxed", once(base, needle, needle.replace("release", "relaxed")), [
            ("ready", FAILURE)]
        needle = "current->dependent_gate.exchange(Dependents::closed,\n                                         rl::memory_order_acq_rel)"
        yield "completion-gate-close-relaxed", once(base, needle, needle.replace("acq_rel", "relaxed")), [
            ("registration", FAILURE)]
        changed = method(base, "void CompletionState::add_dependent(",
                         "rl::memory_order_acquire);", "rl::memory_order_relaxed);")
        yield "completion-registration-failure-relaxed", changed, [("registration", FAILURE)]
        needle = "dependent->credits.fetch_sub(1, rl::memory_order_acq_rel)"
        changed = method(base, "void CompletionState::model_finish_terminal(",
                         needle, needle.replace("acq_rel", "release"))
        changed = method(changed, "void model_deliver_edge(",
                         needle, needle.replace("acq_rel", "release"))
        yield "completion-dependent-retire-release-only", changed, [
            ("cascade", FAILURE)]
    elif protocol == "idle_accounting":
        base = idle_accounting()
        yield "baseline", base, [(s, ("SUCCESS",)) for s in
                                 ("visibility", "aggregate", "handoff", "external-handoff", "wait",
                                  "multiple-waiters", "repeated-waiters")]
        needle = "retired.store(previous + 1, rl::memory_order_release)"
        yield "idle-retire-relaxed", once(base, needle, needle.replace("release", "relaxed")), [
            ("visibility", FAILURE), ("aggregate", FAILURE)]
        needle = ("retired_before.add(\n"
                  "        workers_[worker_index].retired.load(rl::memory_order_acquire))")
        yield "idle-first-collect-relaxed", once(base, needle, needle.replace("acquire", "relaxed")), [
            ("handoff", FAILURE), ("external-handoff", FAILURE)]
        needle = "return retired_before == retired_after && published_total == retired_after;"
        yield "idle-drop-stability-check", once(base, needle,
                                                "return published_total == retired_after;"), [
            ("handoff", FAILURE), ("external-handoff", FAILURE)]
        for name, signature in (("worker", "void IdleAccounting::worker_idle("),
                                ("waiter", "void IdleAccounting::wait(")):
            changed = method(base, signature,
                             "rl::atomic_thread_fence(rl::memory_order_seq_cst);",
                             "/* missing announcement fence */")
            yield f"idle-drop-{name}-fence", changed, [("wait", LOST_WAKE)]
        changed = once(base, "  {\n    rl::lock_guard<rl::mutex> lock(mutex_);\n  }",
                       "  /* no CV enrollment handshake */")
        yield "idle-drop-cv-handoff", changed, [("wait", LOST_WAKE)]
        yield "idle-drop-notify", once(base, "  cv_.notify_all();", "  /* missing notify */"), [
            ("wait", LOST_WAKE)]
        yield "idle-notify-one", once(base, "cv_.notify_all();", "cv_.notify_one();"), [
            ("multiple-waiters", LOST_WAKE)]
        needle = "waiters_.fetch_add(1, rl::memory_order_relaxed)"
        yield "idle-overwrite-waiter-count", once(base, needle,
                                                   "waiters_.store(1, rl::memory_order_relaxed)"), [
            ("multiple-waiters", FAILURE)]
    elif protocol == "shutdown":
        base = shutdown()
        yield "baseline", base, [(s, ("SUCCESS",)) for s in
                                 ("admission", "close-race", "descendant-drain", "publication-drain",
                                  "shutdown-with-waiter")]
        body = definition(base, "void Pool::shutdown(")
        barrier = definition(body, "while (state != admission_closed_bit)")
        changed = once(base, body, once(body, barrier, "/* missing publisher drain */"))
        yield "shutdown-drop-admission-drain", changed, [
            ("admission", FAILURE), ("publication-drain", FAILURE)]
        needle = "admission_.fetch_sub(1, rl::memory_order_release)"
        yield "shutdown-relax-publisher-release", once(base, needle, needle.replace("release", "relaxed")), [
            ("admission", FAILURE)]
        if body.count("rl::memory_order_acquire") != 3:
            raise ValueError("Expected three acquire observations in shutdown")
        changed = once(base, body, body.replace("rl::memory_order_acquire", "rl::memory_order_relaxed"))
        # A documented survivor control: the following accounting SC fence
        # can acquire the release read by the relaxed admission observation.
        yield "shutdown-relax-admission-acquire", changed, [("admission", ("SUCCESS",))]
        changed = method(changed, "void IdleAccounting::wait(",
                         "rl::atomic_thread_fence(rl::memory_order_seq_cst);",
                         "/* also remove the later acquire fence */")
        yield "shutdown-relax-admission-and-wait-fence", changed, [("admission", FAILURE)]
        changed = method(base, "void Pool::shutdown(",
                         "  accounting_->wait();", "  /* missing physical drain */")
        yield "shutdown-drop-physical-drain", changed, [
            ("descendant-drain", FAILURE), ("publication-drain", FAILURE)]
        needle = "  accounting_->wait();\n  stop_.store(true, rl::memory_order_release);"
        changed = once(base, needle,
                       "  stop_.store(true, rl::memory_order_release);\n  accounting_->wait();")
        yield "shutdown-stop-before-drain", changed, [
            ("descendant-drain", FAILURE), ("publication-drain", FAILURE)]
        changed = once(body, "  accounting_->wait();", "  /* drain moved before admission */")
        changed = once(changed, "  close();", "  accounting_->wait();\n  close();")
        yield "shutdown-idle-before-admission", once(base, body, changed), [
            ("publication-drain", FAILURE)]
        changed = method(base, "void Pool::enter_publication(",
                         "if (state & admission_closed_bit)", "if (false)")
        yield "shutdown-ignore-closed", changed, [("close-race", FAILURE)]
        yield "shutdown-notify-one", once(base, "cv_.notify_all();", "cv_.notify_one();"), [
            ("shutdown-with-waiter", LOST_WAKE)]
    else:
        raise ValueError(f"Unknown protocol: {protocol}")


SOURCES = {
    "deque": ["include/dagflow/detail/chase_lev_deque.hpp"],
    "parking": ["include/dagflow/detail/parking_lot.hpp", "src/parking_lot.cpp"],
    "completion": ["include/dagflow/handle.hpp", "src/thread_pool.cpp"],
    "idle_accounting": ["include/dagflow/detail/idle_accounting.hpp", "src/idle_accounting.cpp"],
    "shutdown": ["include/dagflow/detail/idle_accounting.hpp", "src/idle_accounting.cpp",
                 "include/dagflow/thread_pool.hpp", "src/thread_pool.cpp"],
}
