#include "test_runner.h"
#include "profile.h"

#include <vector>
#include <iostream>
#include <future>
#include <map>
#include <functional>
#include <sstream>
#include <numeric>
#include <cmath>
#include <mutex>
#include <queue>
#include <string>
#include <mutex>
#include <random>
#include <memory>
#include <algorithm>

using namespace std;

struct Stats {
    map<string, int> word_frequences;

    void operator += (const Stats& other) {
        for (auto& [word, count] : other.word_frequences) {
            word_frequences[word] += count;
        }
    }
};


Stats ExploreLine(const set<string>& key_words, const string& line) {
    Stats result;
    string word;
    istringstream is(line);
    while (is >> word) {
        if (key_words.find(word) != key_words.end()) {
            result.word_frequences[word]++;
        }
    }
    return result;
}

Stats ExploreLinesVector(const set<string>& key_words, vector<string> input) {
    Stats result;
    for (const auto& line : input) {
        result += ExploreLine(key_words, line);
    }
    return result;
}

vector<string> FetchMore(size_t size, istream& input){
    vector<string> result;
    result.reserve(size);
    int count = 0;
    for (string line; getline(input, line); ++count) {
        result.push_back(move(line));
        if (count == size - 1) break;
    }
    return result;
}

Stats ExploreKeyWords(const set<string>& key_words, istream& input) {
    Stats result;
    const size_t PAGE_SIZE = 10000;
    vector<future<Stats>> interm_results;

    vector<string> strings = FetchMore(PAGE_SIZE, input);
    while (strings.size() > 0) {
        interm_results.push_back(async(
            ExploreLinesVector, key_words, move(strings)));
        strings = FetchMore(PAGE_SIZE, input);
    }

    for (auto& f: interm_results) {
        result += f.get();
    }
    return result;
}

void TestBasic() {
    const set<string> key_words = { "yangle", "rocks", "sucks", "all" };

    stringstream ss;
    ss << "this new yangle service really rocks\n";
    ss << "It sucks when yangle isn't available\n";
    ss << "10 reasons why yangle is the best IT company\n";
    ss << "yangle rocks others suck\n";
    ss << "Goondex really sucks, but yangle rocks. Use yangle\n";

    const auto stats = ExploreKeyWords(key_words, ss);
    const map<string, int> expected = {
      {"yangle", 6},
      {"rocks", 2},
      {"sucks", 1}
    };
    ASSERT_EQUAL(stats.word_frequences, expected);
}


void TestLong() {
    const set<string> key_words = { "yangle", "rocks", "sucks", "all" };
    const int OPERATIONS = 30000;

    stringstream ss;
    for (int i = 0; i < OPERATIONS; ++i) {
        ss << "this new yangle service really rocks\n";
        ss << "It sucks when yangle isn't available\n";
        ss << "10 reasons why yangle is the best IT company\n";
        ss << "yangle rocks others suck\n";
        ss << "Goondex really sucks, but yangle rocks. Use yangle\n";
    }

    const auto stats = ExploreKeyWords(key_words, ss);
    const map<string, int> expected = {
      {"yangle", 6 * OPERATIONS},
      {"rocks", 2 * OPERATIONS},
      {"sucks", OPERATIONS}
    };
    ASSERT_EQUAL(stats.word_frequences, expected);
}

template<typename T>
class Synchronized {
public:
    explicit Synchronized(T initial = T()) {
        value = move(initial);
    }

    struct Access {
        T& ref_to_value;
        lock_guard<mutex> guard;
    };

    Access GetAccess() {
        return { value, lock_guard(m)};
    }
private:
    T value;
    mutex m;
};


void TestConcurrentUpdate() {
    Synchronized<string> common_string;

    const size_t add_count = 50000;
    auto updater = [&common_string, add_count] {
        for (size_t i = 0; i < add_count; ++i) {
            auto access = common_string.GetAccess();
            access.ref_to_value += 'a';
        }
    };

    auto f1 = async(updater);
    auto f2 = async(updater);

    f1.get();
    f2.get();

    ASSERT_EQUAL(common_string.GetAccess().ref_to_value.size(), 2 * add_count);
}


vector<int> Consume(Synchronized<deque<int>>& common_queue) {
    vector<int> got;

    for (;;) {
        deque<int> q;
        {
            auto access = common_queue.GetAccess();
            q = move(access.ref_to_value);
        }

        for (int item : q) {
            if (item > 0) {
                got.push_back(item);
            }
            else {
                return got;
            }
        }
    }
}

void TestProducerConsumer() {
    Synchronized<deque<int>> common_queue;

    auto consumer = async(Consume, ref(common_queue));

    const size_t item_count = 100000;
    for (size_t i = 1; i <= item_count; ++i) {
        common_queue.GetAccess().ref_to_value.push_back(i);
    }
    common_queue.GetAccess().ref_to_value.push_back(-1);

    vector<int> expected(item_count);
    iota(begin(expected), end(expected), 1);
    ASSERT_EQUAL(consumer.get(), expected);
}


template<typename K, typename V>
class ConcurrentMap {
public:
    static_assert(is_integral_v<K>, "ConcurrentMap supports only integer keys");

    struct Bucket {
        map<K, V> submap;
        mutex m;
    };

    struct Access {
        lock_guard<mutex> guard;
        V& ref_to_value;
        Access(const K& key, Bucket& bucket) 
            : guard(bucket.m), ref_to_value(bucket.submap[key])
        {}
    };

    explicit ConcurrentMap(size_t bucket_count)
        : containers_num(bucket_count),
        data_(bucket_count){
    }

    Access operator[](const K& key) {
        size_t num = Index(key);
        return { key, data_[num]};
    }

    map<K, V> BuildOrdinaryMap() {
        map<K, V> result;

        for (size_t i = 0; i < containers_num; ++i) {
            Update(result, i);
        }
        return result;
    }
private:
    vector<Bucket> data_;
    size_t containers_num;
    mutex guard_;

    size_t Index(K position) {
        return position % containers_num;
    }

    void Update(map<K, V>& res, int i) {
        lock_guard<mutex> lock(data_[i].m);
        res.insert(data_[i].submap.begin(), data_[i].submap.end());
    }
};


void RunConcurrentUpdates(
    ConcurrentMap<int, int>& cm, size_t thread_count, int key_count
) {
    auto kernel = [&cm, key_count](int seed) {
        vector<int> updates(key_count);
        iota(begin(updates), end(updates), -key_count / 2);
        shuffle(begin(updates), end(updates), default_random_engine(seed));

        for (int i = 0; i < 2; ++i) {
            for (auto key : updates) {
                cm[key].ref_to_value++;
            }
        }
    };

    vector<future<void>> futures;
    for (size_t i = 0; i < thread_count; ++i) {
        futures.push_back(async(kernel, i));
    }
}

void TestConcurrentUpdate2() {
    const size_t thread_count = 3;
    const size_t key_count = 50000;

    ConcurrentMap<int, int> cm(thread_count);
    RunConcurrentUpdates(cm, thread_count, key_count);

    const auto result = cm.BuildOrdinaryMap();
    ASSERT_EQUAL(result.size(), key_count);
    for (auto& [k, v] : result) {
        AssertEqual(v, 6, "Key = " + to_string(k));
    }
}


void TestReadAndWrite() {
    ConcurrentMap<size_t, string> cm(5);

    auto updater = [&cm] {
        for (size_t i = 0; i < 50000; ++i) {
            cm[i].ref_to_value += 'a';
        }
    };
    auto reader = [&cm] {
        vector<string> result(50000);
        for (size_t i = 0; i < result.size(); ++i) {
            result[i] = cm[i].ref_to_value;
        }
        return result;
    };

    auto u1 = async(updater);
    auto r1 = async(reader);
    auto u2 = async(updater);
    auto r2 = async(reader);

    u1.get();
    u2.get();

    for (auto f : { &r1, &r2 }) {
        auto result = f->get();
        ASSERT(all_of(result.begin(), result.end(), [](const string& s) {
            return s.empty() || s == "a" || s == "aa";
        }));
    }
}

void TestSpeedup() {
    {
        ConcurrentMap<int, int> single_lock(1);

        LOG_DURATION("Single lock");
        RunConcurrentUpdates(single_lock, 4, 50000);
    }
    {
        ConcurrentMap<int, int> many_locks(100);

        LOG_DURATION("100 locks");
        RunConcurrentUpdates(many_locks, 4, 50000);
    }
}


int main()
{
    TestRunner tr;
    LOG_DURATION("Total");
    RUN_TEST(tr, TestBasic);
    {
        LOG_DURATION("long test");
        RUN_TEST(tr, TestLong);
    }

    RUN_TEST(tr, TestConcurrentUpdate);
    RUN_TEST(tr, TestProducerConsumer);

    RUN_TEST(tr, TestConcurrentUpdate2);
    RUN_TEST(tr, TestReadAndWrite);
    RUN_TEST(tr, TestSpeedup);
    std::cout << "Hello World!\n";
}
