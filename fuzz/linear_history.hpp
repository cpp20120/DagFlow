#pragma once
#include <array>
#include <cstdint>
#include <deque>
#include <dagflow/detail/ring_mpmc.hpp>
#include "check.hpp"
namespace dagflow::fuzz {
struct Operation {
  bool push{}, success{};
  unsigned input{}, result{}, begin{}, end{};
};
// Bounded exhaustive linearizability checker for short complete histories.
// Timestamp ordering records real-time non-overlap only; overlapping methods
// may be linearized in either order. Independent of the queue implementation.
inline bool linearizable(const std::array<Operation,8>& log,
                         unsigned mask, std::deque<unsigned>& fifo) {
  if(mask==255u) return true;
  for(unsigned i=0;i<8;++i) {
    if(mask&(1u<<i))continue;
    bool precedes=false;
    for(unsigned j=0;j<8;++j)
      if(!(mask&(1u<<j)) && j!=i && log[j].end<log[i].begin)
        precedes=true;
    if(precedes)continue;
    const auto& op=log[i];
    auto next=fifo;
    if(op.push) {
      if(op.success!=(next.size()<2))continue;
      if(op.success)next.push_back(op.input);
    } else {
      if(op.success!=!next.empty())continue;
      if(op.success) {
        if(op.result!=next.front())continue;
        next.pop_front();
      }
    }
    if(linearizable(log,mask|(1u<<i),next))return true;
  }
  return false;
}
}
