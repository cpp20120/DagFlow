#pragma once

// The shutdown body reaches its actual wake_all call after publishing stop.
// Inspect effects here, before any real thread join could hide a missing edge.
struct ModelParking {
  void (*inspect)(void*) = nullptr;
  void* context = nullptr;
  void wake_all() {
    if (inspect) inspect(context);
  }
};

// Thread creation, parking and OS join are outside the bounded composition.
// Storage is pinned by the Relacy fixture until all actors finish.
struct ModelThread {
  bool joinable() const { return false; }
  void join() { RL_ASSERT(false); }
};
