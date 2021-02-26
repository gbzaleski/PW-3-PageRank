
// Grzegorz B. Zaleski (418494)

#ifndef SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_
#define SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_

#include <atomic>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <condition_variable>
#include <cmath>

#include "immutable/network.hpp"
#include "immutable/pageIdAndRank.hpp"
#include "immutable/pageRankComputer.hpp"

class MultiThreadedPageRankComputer : public PageRankComputer
{
    private:

    // Klasa bariery wieloużytkowej.
    class cyclicBarrier
    {
        private:
        long long limit;
        long long current;
        long long iteration;
        std::mutex mut;
        std::condition_variable cv;

        public:
        explicit cyclicBarrier(long long _limit)
        {
            limit = _limit;
            current = 0;
            iteration = -1;
        }

        void reach(long long my_iteration)
        {
            std::unique_lock<std::mutex> lock(mut);
            current++;

            if (current == limit)
            {
                current = 0;
                iteration++;
                cv.notify_all();
                return;
            }

            cv.wait(lock, [&] {return my_iteration == iteration;});
        }
    };

    uint32_t numThreads;
    mutable uint32_t networkSize;
    mutable double difference;
    mutable double dangleSum;
    mutable std::mutex modifyMutex;
    mutable std::mutex iteratorMutex;
    mutable size_t push;

    void threadCompute(const double alpha, const uint32_t iterations, const double tolerance, const size_t orderId,
                       cyclicBarrier &barrier,
                       std::unordered_map<PageId, PageRank, PageIdHash> &pageHashMap,
                       std::unordered_map<PageId, PageRank, PageIdHash> &previousPageHashMap,
                       std::unordered_map<PageId, uint32_t, PageIdHash> &numLinks,
                       std::vector<PageId> &danglingNodes,
                       std::unordered_map<PageId, std::vector<PageId>, PageIdHash> &edges,
                       std::unordered_map<PageId, PageRank, PageIdHash>::iterator &pageHashMapIter,
                       std::vector<PageId>::iterator &danglingNodeIter,
                       size_t &pagesItr,
                       const Network &network) const
    {
        // Lokalne własne zmienne każdego threada.
        double localDifference, localDangleSum;
        std::vector<PageId>::iterator dangler;
        std::unordered_map<PageId, PageRank, PageIdHash>::iterator node;
        size_t my_push = 0;
        size_t pager = 0;
        long long barrierIter = 0;

        // Współbieżne generowanie ID
        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(iteratorMutex);
                if (pagesItr == network.getPages().size())
                    break;

                pager = pagesItr;
                pagesItr++;
            }

            network.getPages()[pager].generateId(network.getGenerator());
            {
                std::lock_guard<std::mutex> lock(modifyMutex);
                pageHashMap[network.getPages()[pager].getId()] = 1.0 / networkSize;
            }
        }
        barrier.reach(barrierIter++);

        pagesItr = 0;

        barrier.reach(barrierIter++);

        // Współbieżne zliczanie połączeń
        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(iteratorMutex);
                if (pagesItr == network.getPages().size())
                    break;

                pager = pagesItr;
                pagesItr++;
            }

            {
                std::lock_guard<std::mutex> lock(modifyMutex);
                numLinks[network.getPages()[pager].getId()] = network.getPages()[pager].getLinks().size();
            }
        }
        barrier.reach(barrierIter++);

        pagesItr = 0;

        barrier.reach(barrierIter++);

        // Współbieżne zliczanie wierzchołków izolowanych.
        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(iteratorMutex);
                if (pagesItr == network.getPages().size())
                    break;

                pager = pagesItr;
                pagesItr++;
            }

            if (network.getPages()[pager].getLinks().empty())
            {
                std::lock_guard<std::mutex> lock(modifyMutex);
                danglingNodes.emplace_back(network.getPages()[pager].getId());
            }
        }
        barrier.reach(barrierIter++);

        pagesItr = 0;

        barrier.reach(barrierIter++);

        // Współbieżne tworzenie grafu połączeń.
        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(iteratorMutex);
                if (pagesItr == network.getPages().size())
                    break;

                pager = pagesItr;
                pagesItr++;
            }

            for (const auto &link: network.getPages()[pager].getLinks())
            {
                std::lock_guard<std::mutex> lock(modifyMutex);
                edges[link].push_back(network.getPages()[pager].getId());
            }
        }
        barrier.reach(barrierIter++);

        for (uint32_t i = 0; i < iterations; ++i)
        {
            barrier.reach(barrierIter++);

            if (orderId == 0)
            {
                danglingNodeIter = danglingNodes.begin();
                pageHashMapIter = pageHashMap.begin();
                previousPageHashMap = pageHashMap;
                dangleSum = 0;
                difference = 0;
            }

            barrier.reach(barrierIter++);

            // Współbieżne liczenie danglingSum
            localDangleSum = 0;
            while (true)
            {
                if (my_push == 0)
                {
                    std::lock_guard<std::mutex> lock(iteratorMutex);
                    if (danglingNodeIter == danglingNodes.end())
                        break;

                    dangler = danglingNodeIter;
                    while (danglingNodeIter != danglingNodes.end() && my_push < push)
                    {
                        my_push++;
                        danglingNodeIter++;
                    }
                }
                else
                {
                    dangler++;
                }
                my_push--;

                localDangleSum += previousPageHashMap[*dangler];
            }

            {
                std::lock_guard<std::mutex> lock(modifyMutex);
                dangleSum += localDangleSum;
            }

            barrier.reach(barrierIter++);

            // Wspołbieżne liczenie wartości PageRank dla każdego wierzchołka.
            localDifference = 0;
            while (true)
            {
                if (my_push == 0)
                {
                    std::lock_guard<std::mutex> lock(iteratorMutex);
                    if (pageHashMapIter == pageHashMap.end())
                        break;

                    node = pageHashMapIter;
                    while (pageHashMapIter != pageHashMap.end() && my_push < push)
                    {
                        my_push++;
                        pageHashMapIter++;
                    }
                }
                else
                {
                    node++;
                }
                my_push--;

                PageRank currentValue = alpha * dangleSum / networkSize + (1.0 - alpha) / networkSize;

                if (edges.count(node->first) > 0)
                {
                    for (auto const &link: edges[node->first])
                    {
                        currentValue += alpha * previousPageHashMap[link] / numLinks[link];
                    }
                }

                pageHashMap[node->first] = currentValue;
                localDifference += std::abs(previousPageHashMap[node->first] - pageHashMap[node->first]);
            }

            {
                std::lock_guard<std::mutex> lock(modifyMutex);
                difference += localDifference;
            }

            barrier.reach(barrierIter++);
            if (difference < tolerance)
            {
                return;
            }
        }
    }

    public:
    explicit MultiThreadedPageRankComputer(uint32_t numThreadsArg)
            : numThreads(numThreadsArg) {};

    std::vector<PageIdAndRank> computeForNetwork(Network const &network, double alpha, uint32_t iterations, double tolerance) const override
    {
        networkSize = network.getSize();
        difference = 0;
        dangleSum = 0;

        // Optymalizacja dzielenia porcji zadań.
        push = 1 + (size_t) ((double) sqrt(networkSize) / M_E);

        // Tworzenie pomocniczych - współdzielonych - struktur danych.
        cyclicBarrier barrier(numThreads);
        std::unordered_map<PageId, PageRank, PageIdHash> pageHashMap;
        std::unordered_map<PageId, PageRank, PageIdHash> previousPageHashMap;
        std::unordered_map<PageId, uint32_t, PageIdHash> numLinks;
        std::unordered_map<PageId, std::vector<PageId>, PageIdHash> edges;
        std::vector<PageId> danglingNodes;

        auto danglingNodeIter = danglingNodes.begin();
        auto pageHashMapIter = pageHashMap.begin();
        size_t pagesItr = 0;

        std::vector<std::thread> threads;
        for (uint32_t i = 0; i < numThreads; ++i)
        {
            threads.emplace_back(&MultiThreadedPageRankComputer::threadCompute, this, alpha, iterations, tolerance, i,
                                 std::ref(barrier), std::ref(pageHashMap), std::ref(previousPageHashMap),
                                 std::ref(numLinks), std::ref(danglingNodes), std::ref(edges),
                                 std::ref(pageHashMapIter), std::ref(danglingNodeIter), std::ref(pagesItr), std::cref(network));
        }

        for (uint32_t i = 0; i < numThreads; ++i)
        {
            threads[i].join();
        }

        std::vector<PageIdAndRank> result;
        result.reserve(pageHashMap.size());

        for (const auto &iter: pageHashMap)
        {
            result.emplace_back(iter.first, iter.second);
        }

        return result;
    }

    std::string getName() const override
    {
        return "MultiThreadedPageRankComputer[" + std::to_string(this->numThreads) + "]";
    }
};

#endif /* SRC_MULTITHREADEDPAGERANKCOMPUTER_HPP_ */
