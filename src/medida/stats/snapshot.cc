//
// Copyright (c) 2012 Daniel Lundin
//

#include "medida/stats/snapshot.h"

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <numeric>
#include <stdexcept>

namespace medida
{
namespace stats
{

static const double kMEDIAN_Q = 0.5;
static const double kP75_Q = 0.75;
static const double kP95_Q = 0.95;
static const double kP98_Q = 0.98;
static const double kP99_Q = 0.99;
static const double kP999_Q = 0.999;

class Snapshot::Impl
{
  public:
    Impl(const std::vector<WeightedValue>& values);
    ~Impl();
    std::size_t size() const;
    double getValue(double quantile) const;
    double getMedian() const;
    double get75thPercentile() const;
    double get95thPercentile() const;
    double get98thPercentile() const;
    double get99thPercentile() const;
    double get999thPercentile() const;
    std::vector<double> getValues() const;

  private:
    std::vector<WeightedValue> values_;
    double totalWeight_;
};

Snapshot::Snapshot(const std::vector<WeightedValue>& values)
    : impl_{new Snapshot::Impl{values}}
{
}

Snapshot::Snapshot(Snapshot&& other) : impl_{std::move(other.impl_)}
{
}

Snapshot::~Snapshot()
{
}

void
Snapshot::checkImpl() const
{
    if (!impl_)
    {
        throw std::runtime_error("Access to moved Snapshot::impl_");
    }
}

std::size_t
Snapshot::size() const
{
    checkImpl();
    return impl_->size();
}

std::vector<double>
Snapshot::getValues() const
{
    checkImpl();
    return impl_->getValues();
}

double
Snapshot::getValue(double quantile) const
{
    checkImpl();
    return impl_->getValue(quantile);
}

double
Snapshot::getMedian() const
{
    checkImpl();
    return impl_->getMedian();
}

double
Snapshot::get75thPercentile() const
{
    checkImpl();
    return impl_->get75thPercentile();
}

double
Snapshot::get95thPercentile() const
{
    checkImpl();
    return impl_->get95thPercentile();
}

double
Snapshot::get98thPercentile() const
{
    checkImpl();
    return impl_->get98thPercentile();
}

double
Snapshot::get99thPercentile() const
{
    checkImpl();
    return impl_->get99thPercentile();
}

double
Snapshot::get999thPercentile() const
{
    checkImpl();
    return impl_->get999thPercentile();
}

// === Implementation ===

Snapshot::Impl::Impl(const std::vector<WeightedValue>& v)
{
    auto values(v);
    std::sort(std::begin(values), std::end(values));
    values_.reserve(values.size());

    if (values.empty())
    {
        return;
    }
    values_.emplace_back(values.front());
    auto last = values_.begin();
    totalWeight_ = last->weight;
    for (auto it = ++values.begin(); it != values.end(); it++)
    {
        totalWeight_ += it->weight;
        if (it->value == last->value)
        {
            last->value += it->weight;
        }
        else
        {
            values_.emplace_back(*it);
            last++;
        }
    }
}

Snapshot::Impl::~Impl()
{
}

std::size_t
Snapshot::Impl::size() const
{
    return values_.size();
}

std::vector<double>
Snapshot::Impl::getValues() const
{
    std::vector<double> res;
    res.reserve(values_.size());
    std::transform(values_.begin(), values_.end(), std::back_inserter(res),
                   [](WeightedValue const& v) { return v.value; });
    return res;
}

double
Snapshot::Impl::getValue(double quantile) const
{
    if (quantile < 0.0 || quantile > 1.0)
    {
        throw std::invalid_argument("quantile is not in [0..1]");
    }

    if (values_.empty() || totalWeight_ == 0.0)
    {
        return 0.0;
    }

    auto qWeight = quantile * totalWeight_;

    double curQ = 0.0;
    for (auto it = values_.begin(); it != values_.end(); it++)
    {
        auto prevQ = curQ;
        curQ += it->weight;
        if (curQ >= qWeight)
        {
            if (it == values_.begin())
            {
                return it->value;
            }
            auto cur = it--;
            auto v =
                it->value + (qWeight - prevQ) * (cur->value - it->value) / (curQ - prevQ);
            return v;
        }
    }
    return values_.back().value;
}

double
Snapshot::Impl::getMedian() const
{
    return getValue(kMEDIAN_Q);
}

double
Snapshot::Impl::get75thPercentile() const
{
    return getValue(kP75_Q);
}

double
Snapshot::Impl::get95thPercentile() const
{
    return getValue(kP95_Q);
}

double
Snapshot::Impl::get98thPercentile() const
{
    return getValue(kP98_Q);
}

double
Snapshot::Impl::get99thPercentile() const
{
    return getValue(kP99_Q);
}

double
Snapshot::Impl::get999thPercentile() const
{
    return getValue(kP999_Q);
}

} // namespace stats
} // namespace medida
