// Copyright 2020 Stellar Development Foundation and contributors. Licensed
// under the Apache License, Version 2.0. See the COPYING file at the root
// of this distribution or at http://www.apache.org/licenses/LICENSE-2.0

#include "medida/stats/sliding_window_sample.h"

#include <cassert>
#include <chrono>
#include <deque>
#include <mutex>
#include <random>

#include "medida/stats/snapshot.h"

namespace medida
{
namespace stats
{

class SlidingWindowSample::Impl
{
  public:
    Impl(std::size_t windowSize, std::chrono::seconds windowTime);
    ~Impl();
    void Clear();
    std::uint64_t size();
    void Update(std::int64_t value);
    void Update(std::int64_t value, Clock::time_point timestamp);
    Snapshot MakeSnapshot();

  private:
    std::mutex mutex_;
    const std::size_t windowSize_;
    const std::chrono::seconds windowTime_;
    const std::chrono::microseconds timeSlice_;
    std::uint32_t sliceCount_;
    std::default_random_engine rng_;
    std::uniform_int_distribution<std::uint32_t> dist_;
    std::deque<std::pair<double, Clock::time_point>> values_;
};

SlidingWindowSample::SlidingWindowSample(std::size_t windowSize,
                                         std::chrono::seconds windowTime)
    : impl_{new SlidingWindowSample::Impl{windowSize, windowTime}}
{
}

SlidingWindowSample::~SlidingWindowSample()
{
}

void
SlidingWindowSample::Clear()
{
    impl_->Clear();
}

std::uint64_t
SlidingWindowSample::size() const
{
    return impl_->size();
}

void
SlidingWindowSample::Update(std::int64_t value)
{
    impl_->Update(value);
}

void
SlidingWindowSample::Update(std::int64_t value, Clock::time_point timestamp)
{
    impl_->Update(value, timestamp);
}

Snapshot
SlidingWindowSample::MakeSnapshot() const
{
    return impl_->MakeSnapshot();
}

// === Implementation ===

SlidingWindowSample::Impl::Impl(std::size_t windowSize,
                                std::chrono::seconds windowTime)
    : windowSize_(windowSize)
    , windowTime_(windowTime)
    , timeSlice_(
          std::chrono::duration_cast<std::chrono::microseconds>(windowTime) /
          windowSize)
    , sliceCount_(0)
    , rng_(std::random_device()())
    , dist_(0, std::numeric_limits<std::uint32_t>::max())
{
    Clear();
}

SlidingWindowSample::Impl::~Impl()
{
}

void
SlidingWindowSample::Impl::Clear()
{
    std::lock_guard<std::mutex> lock{mutex_};
    values_.clear();
}

std::uint64_t
SlidingWindowSample::Impl::size()
{
    std::lock_guard<std::mutex> lock{mutex_};
    return values_.size();
}

void
SlidingWindowSample::Impl::Update(std::int64_t value)
{
    Update(value, Clock::now());
}

void
SlidingWindowSample::Impl::Update(std::int64_t value,
                                  Clock::time_point timestamp)
{
    std::lock_guard<std::mutex> lock{mutex_};

    if (!values_.empty())
    {
        // If we're in a new timeslice, reset count
        if (timestamp > values_.back().second + timeSlice_)
        {
            sliceCount_ = 0;
        }

        // If there's old data, trim it.
        Clock::time_point expiryTime = timestamp - windowTime_;
        while (!values_.empty() && values_.front().second < expiryTime)
        {
            values_.pop_front();
        }
    }

    // When you add samples to the sliding window _slowly_ nothing goes wrong;
    // when you add them too _quickly_ there's the possibility of losing rare
    // events because they're overwritten before they get observed. To
    // compensate for this, XXXXX
    // XXXXXXXXXX

    // Check if we've already inserted an item for the same timeSlice.
    if (!values_.empty() && timestamp <= values_.back().second + timeSlice_)
    {
        sliceCount_++;
        auto r = size_t(dist_(rng_))*sliceCount_;
        if (r <= std::numeric_limits<std::uint32_t>::max())
        {
            // Keep old timestamp to anchor timeSlice; but replace value.
            values_.back().first = value;
        }
    }
    else
    {
        values_.emplace_back(value, timestamp);
        sliceCount_ = 1;
        if (values_.size() > windowSize_)
        {
            values_.pop_front();
        }
    }
}

Snapshot
SlidingWindowSample::Impl::MakeSnapshot()
{
    std::lock_guard<std::mutex> lock{mutex_};
    std::vector<double> vals;
    vals.reserve(values_.size());
    for (auto v : values_)
    {
        vals.emplace_back(v.first);
    }
    return {vals};
}

} // namespace stats
} // namespace medida
