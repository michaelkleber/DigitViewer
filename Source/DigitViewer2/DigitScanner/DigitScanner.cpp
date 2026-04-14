/* DigitScanner.cpp
 * 
 * Author           : Michael Kleber
 * Date Created     : 01/15/2026
 * Last Modified    : 04/07/2026
 * Copyright 2026 Google LLC
 * 
 */

#include <algorithm>
#include <chrono>
#include <atomic>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <time.h>
#include <cstdio>

#if defined(_MSC_VER)
#include <intrin.h>
// MSVC: Map to the T0 hint (fetch to all cache levels), read-only.
#define PREFETCH(addr) _mm_prefetch((const char*)(addr), _MM_HINT_T0)
#elif defined(__GNUC__) || defined(__clang__)
// GCC/Clang: Map to the builtin with read-only and high locality
#define PREFETCH(addr) __builtin_prefetch((addr), 0, 3)
#else
// Do nothing on other compilers
#define PREFETCH(addr) ((void)0)
#endif

#include "PublicLibs/ConsoleIO/BasicIO.h"
#include "PublicLibs/BasicLibs/StringTools/ToString.h"
#include "PublicLibs/BasicLibs/Memory/SmartBuffer.h"
#include "PublicLibs/SystemLibs/Concurrency/Parallelizers.h"
#include "PublicLibs/SystemLibs/Environment/Environment.h"
#ifdef YMP_STANDALONE
#include "PrivateLibs/SystemLibs/ParallelFrameworks/ParallelFrameworks.h"
#endif
#include "DigitViewer2/Globals.h"
#include "DigitViewer2/RawToAscii/RawToAscii.h"
#include "DigitViewer2/DigitReaders/BasicDigitReader.h"
#include "DigitScanner.h"



namespace DigitViewer2 {

using namespace ymp;

static double get_cpu_time(){
#if defined(__GNUC__) || defined(__clang__)
struct timespec ts;
    if (clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &ts) == 0){
        return (double)ts.tv_sec + (double)ts.tv_nsec / 1000000000.0;
    }
#endif
    return 0;
}

static std::string format_times(double wall_time, double cpu_time) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%.3fs (wall), %.3fs (CPU)", wall_time, cpu_time);
    return std::string(buffer);
}

class DigitsMapperAction : public ymp::BasicAction {
public:
    DigitsMapperAction(
        BasicDigitReader& reader,
        std::vector<std::vector<std::vector<uint64_t>>>& buckets,
        uiL_t radix_to_d_minus_1,
        upL_t digits,
        char radix,
        uiL_t current_stream_offset,
        uiL_t chunk_to_process,
        upL_t num_threads,
        uiL_t partition_size_bits,
        uiL_t recommended_bucket_capacity
    )
        : m_reader(reader)
        , m_buckets(buckets)
        , m_radix_to_d_minus_1(radix_to_d_minus_1)
        , m_d(digits)
        , m_radix(radix)
        , m_current_stream_offset(current_stream_offset)
        , m_chunk_to_process(chunk_to_process)
        , m_num_threads(num_threads)
        , m_partition_size_bits(partition_size_bits)
        , m_recommended_bucket_capacity(recommended_bucket_capacity)
    {}

    virtual void run(upL_t index = 0) override {
        upL_t block_size = (m_chunk_to_process + m_num_threads - 1) / m_num_threads;
        uiL_t start_offset = m_current_stream_offset + index * block_size;
        uiL_t end_offset = std::min(start_offset + block_size, m_current_stream_offset + m_chunk_to_process);
        if (start_offset >= end_offset) return;

        auto& my_buckets = m_buckets[index];

        // Clear my row in buckets (one bucket for each partition)
        for (upL_t j = 0; j < m_num_threads; ++j) {
            auto& bucket = my_buckets[j];
            bucket.clear();
            if (bucket.capacity() < m_recommended_bucket_capacity) {
                bucket.reserve(m_recommended_bucket_capacity);
            }
        }

        upL_t bytes = m_reader.recommend_buffer_size(end_offset - start_offset);
        SmartBuffer<> buffer(bytes, BUFFER_ALIGNMENT);
        std::vector<char> raw_digits(end_offset - start_offset);
        AlignedBufferC<BUFFER_ALIGNMENT> frame(buffer, bytes);
        
        uiL_t thread_d_string_value = 0;
        upL_t seed_digits = m_d - 1;
        if (start_offset > 0) {
            std::vector<char> seed_raw(seed_digits);
            m_reader.load_digits(&seed_raw[0], nullptr, start_offset - seed_digits, seed_digits, frame, parallelizer_none, 1);
            std::vector<char> seed_dec(seed_digits);
            RawToAscii::raw_to_dec(&seed_dec[0], &seed_raw[0], seed_digits);

            for (upL_t i = 0; i < seed_digits; ++i) {
                thread_d_string_value = thread_d_string_value * m_radix + (seed_dec[i] - '0');
            }
        }

        m_reader.load_digits(&raw_digits[0], nullptr, start_offset, end_offset - start_offset, frame, parallelizer_none, 1);
        std::vector<char> dec_digits(end_offset - start_offset);
        RawToAscii::raw_to_dec(&dec_digits[0], &raw_digits[0], end_offset - start_offset);

        uiL_t string_value = thread_d_string_value;
        upL_t num_digits = end_offset - start_offset;

        for (upL_t i = 0; i < num_digits; ++i) {
            char digit = dec_digits[i];
            string_value = (string_value % m_radix_to_d_minus_1) * m_radix + (digit - '0');
            
            uiL_t val = string_value;
            uiL_t partition_id = val / m_partition_size_bits;
            if (partition_id >= m_num_threads) partition_id = m_num_threads - 1;

            my_buckets[partition_id].push_back(val);
        }
    }

private:
    BasicDigitReader& m_reader;
    std::vector<std::vector<std::vector<uint64_t>>>& m_buckets;
    uiL_t m_radix_to_d_minus_1;
    upL_t m_d;
    char m_radix;
    uiL_t m_current_stream_offset;
    uiL_t m_chunk_to_process;
    upL_t m_num_threads;
    uiL_t m_partition_size_bits;
    uiL_t m_recommended_bucket_capacity;
};

class ValuesReducerAction : public ymp::BasicAction {
public:
    ValuesReducerAction(
        std::vector<uint64_t>& seen_strings,
        const std::vector<std::vector<std::vector<uint64_t>>>& buckets,
        std::atomic<uiL_t>& found_strings_count,
        upL_t num_threads
    )
        : m_seen_strings(seen_strings)
        , m_buckets(buckets)
        , m_found_strings_count(found_strings_count)
        , m_num_threads(num_threads)
    {}

    virtual void run(upL_t index = 0) override {
        upL_t j = index;
        if (j >= m_num_threads) return;

        uiL_t local_new_bits = 0;
        const int PREFETCH_DIST = 16;

        // Directly iterate over buckets without merging or sorting
        for (upL_t i = 0; i < m_num_threads; ++i) {
            const auto& bucket = m_buckets[i][j];
            
            for (size_t k = 0; k < bucket.size(); ++k) {
                if (k + PREFETCH_DIST < bucket.size()) {
                    uiL_t future_val = bucket[k + PREFETCH_DIST];
                    PREFETCH(&m_seen_strings[future_val / 64]);
                }

                uiL_t val = bucket[k];
                uiL_t idx = val / 64;
                uint64_t mask = 1ULL << (val % 64);
                
                uint64_t old_val = m_seen_strings[idx];
                if (!(old_val & mask)) {
                    m_seen_strings[idx] = old_val | mask;
                    local_new_bits++;
                }
            }
        }

        m_found_strings_count.fetch_add(local_new_bits, std::memory_order_relaxed);
    }

private:
    std::vector<uint64_t>& m_seen_strings;
    const std::vector<std::vector<std::vector<uint64_t>>>& m_buckets;
    std::atomic<uiL_t>& m_found_strings_count;
    upL_t m_num_threads;
};

class DigitMapScanAction : public ymp::BasicAction {
public:
    DigitMapScanAction(
        BasicDigitReader& reader,
        std::unordered_map<uiL_t, std::vector<uiL_t>>& missing_strings_map,
        const std::vector<uint64_t>& bloom_filter,
        uiL_t bloom_mask,
        std::mutex& map_mutex,
        std::atomic<uiL_t>& unfound_count,
        uiL_t radix_to_d_minus_1,
        upL_t digits,
        char radix,
        uiL_t current_stream_offset,
        uiL_t chunk_to_process,
        upL_t num_threads
    )
        : m_reader(reader)
        , m_missing_strings_map(missing_strings_map)
        , m_bloom_filter(bloom_filter)
        , m_bloom_mask(bloom_mask)
        , m_map_mutex(map_mutex)
        , m_unfound_count(unfound_count)
        , m_radix_to_d_minus_1(radix_to_d_minus_1)
        , m_d(digits)
        , m_radix(radix)
        , m_current_stream_offset(current_stream_offset)
        , m_chunk_to_process(chunk_to_process)
        , m_num_threads(num_threads)
    {}

    virtual void run(upL_t index = 0) override {
        upL_t block_size = (m_chunk_to_process + m_num_threads - 1) / m_num_threads;
        uiL_t start_offset = m_current_stream_offset + index * block_size;
        uiL_t end_offset = std::min(start_offset + block_size, m_current_stream_offset + m_chunk_to_process);
        if (start_offset >= end_offset) return;

        // Each thread needs its own buffer
        upL_t bytes = m_reader.recommend_buffer_size(end_offset - start_offset);
        SmartBuffer<> buffer(bytes, BUFFER_ALIGNMENT);
        std::vector<char> raw_digits(end_offset - start_offset);
        AlignedBufferC<BUFFER_ALIGNMENT> frame(buffer, bytes);
        
        // Each thread seeds its sliding window, using the d-1 digits before its range of digits begins
        uiL_t thread_d_string_value = 0;
        upL_t seed_digits = m_d - 1;
        if (start_offset > 0) {
            std::vector<char> seed_raw(seed_digits);
            m_reader.load_digits(&seed_raw[0], nullptr, start_offset - seed_digits, seed_digits, frame, parallelizer_none, 1);
            std::vector<char> seed_dec(seed_digits);
            RawToAscii::raw_to_dec(&seed_dec[0], &seed_raw[0], seed_digits);

            for (upL_t i = 0; i < seed_digits; ++i) {
                thread_d_string_value = thread_d_string_value * m_radix + (seed_dec[i] - '0');
            }
        }

        m_reader.load_digits(&raw_digits[0], nullptr, start_offset, end_offset - start_offset, frame, parallelizer_none, 1);
        std::vector<char> dec_digits(end_offset - start_offset);
        RawToAscii::raw_to_dec(&dec_digits[0], &raw_digits[0], end_offset - start_offset);

        for (upL_t i = 0; i < (end_offset - start_offset); ++i) {
            char current_digit = dec_digits[i];
            thread_d_string_value = (thread_d_string_value % m_radix_to_d_minus_1) * m_radix + (current_digit - '0');
            
            // Bloom Filter Check
            uiL_t bloom_idx = thread_d_string_value & m_bloom_mask;
            if ((m_bloom_filter[bloom_idx / 64] >> (bloom_idx % 64)) & 1ULL) {
                // Check if this string is in our map of missing values
                auto it = m_missing_strings_map.find(thread_d_string_value);
                if (it != m_missing_strings_map.end()) {
                    std::lock_guard<std::mutex> lock(m_map_mutex);
                    if (it->second.empty()) {
                        m_unfound_count.fetch_sub(1, std::memory_order_relaxed);
                    }
                    it->second.push_back(start_offset + i + 1);
                }
            }
        }
    }

private:
    BasicDigitReader& m_reader;
    std::unordered_map<uiL_t, std::vector<uiL_t>>& m_missing_strings_map;
    const std::vector<uint64_t>& m_bloom_filter;
    uiL_t m_bloom_mask;
    std::mutex& m_map_mutex;
    std::atomic<uiL_t>& m_unfound_count;
    uiL_t m_radix_to_d_minus_1;
    upL_t m_d;
    char m_radix;
    uiL_t m_current_stream_offset;
    uiL_t m_chunk_to_process;
    upL_t m_num_threads;
};

DigitScanner::DigitScanner(BasicDigitReader& reader, upL_t d)
    : m_reader(reader)
    , m_d(d)
{}

void DigitScanner::search() {
    auto start_time = std::chrono::high_resolution_clock::now();
    double start_cpu = get_cpu_time();

    // Calculate the total number of possible d-digit strings (10^d).
    uiL_t total_strings = 1;
    for (upL_t i = 0; i < m_d; ++i) {
        total_strings *= m_reader.radix();
    }

    //  Check if there is enough memory for the bit vector.
    uiL_t num_atomic_words = (total_strings + 63) / 64;
    uiL_t required_bytes = num_atomic_words * sizeof(std::atomic<uint64_t>);
    uiL_t free_bytes = Environment::GetFreePhysicalMemory();
    if (required_bytes > free_bytes){
      Console::println("Error: Not enough memory.", 'R');
      Console::println("  Required Memory: " + StringTools::tostr(required_bytes, StringTools::COMMAS) + " bytes", 'R');
      Console::println("  Available Memory: " + StringTools::tostr(free_bytes, StringTools::COMMAS) + " bytes", 'R');
      Console::println();
      return;
    }

    //  Use atomic vector for thread-safe bitmask operations.
    //  Use atomic vector for thread-safe bitmask operations.
    std::vector<uint64_t> seen_strings(num_atomic_words, 0);
    
    //  Use atomic counters for thread-safe updates.
    std::atomic<uiL_t> found_strings_count(0);
    std::atomic<uiL_t> last_found_digit_pos(0);
    std::atomic<uiL_t> last_found_d_string(0);
    uiL_t current_offset = 0;
    uiL_t digits_since_last_report = 0 ;
    
    //  Prepare for reading digits in blocks.
    uiL_t limit = m_reader.stream_end();
    if (limit == 0){
        limit = (uiL_t)0 - 1;
    }

    //  Not enough digits to form a d-digit string.
    if (limit < m_d){
        Console::println("Warning: Not enough digits in the stream to form a d-digit string.", 'Y');
        Console::println();
        return;
    }

    // Calculate 10^(d-1) to help with the sliding window
    uiL_t radix_to_d_minus_1 = 1;
    if (m_d > 1) {
        for (upL_t i = 0; i < m_d - 1; ++i) {
            radix_to_d_minus_1 *= m_reader.radix();
        }
    }

    Console::println("Scanning for d-digit strings...");

    current_offset = m_d - 1;
    digits_since_last_report = 0;

    const upL_t MAX_PARALLEL_CHUNK_SIZE = 100000000;
    
    // Use floor(95% of available cores)
    upL_t effective_threads = (Environment::GetLogicalProcessors() * 95) / 100;
    if (effective_threads == 0) effective_threads = 1;
    Console::println("Using " + StringTools::tostr(effective_threads) + " threads for parallel processing.");

    // Calculate partition size for partitions, aligned to 512 bits (cache line)
    uiL_t partition_size_bits = ((total_strings / effective_threads) / 512) * 512;
    if (partition_size_bits == 0) partition_size_bits = 512; // fallback for small total_strings

    // Pre-allocate buckets and merge lists
    std::vector<std::vector<std::vector<uint64_t>>> buckets(effective_threads, std::vector<std::vector<uint64_t>>(effective_threads));

    uiL_t recommended_bucket_capacity = (MAX_PARALLEL_CHUNK_SIZE * 12) / (effective_threads * effective_threads * 10);
    if (recommended_bucket_capacity < 1000) recommended_bucket_capacity = 1000;

    // Scan phase 1: Use a bitvector to record which of the 10^d strings have been seen.
    while (found_strings_count.load(std::memory_order_acquire) < total_strings && current_offset < limit) {
        uiL_t found_count = found_strings_count.load(std::memory_order_relaxed);
        uiL_t unfound_count = total_strings - found_count;

        if (unfound_count < 100000){
            // Switch to Map-based scan
            auto current_time = std::chrono::high_resolution_clock::now();
            double current_cpu = get_cpu_time();
            std::chrono::duration<double> elapsed = current_time - start_time;
            Console::println("\n" + StringTools::tostr(unfound_count) +
                             " strings remaining. Switching to map-based parallel mode. Time: " +
                             format_times(elapsed.count(), current_cpu - start_cpu));
            break;
        }

        uiL_t chunk_to_process = std::min((uiL_t)MAX_PARALLEL_CHUNK_SIZE, limit - current_offset);

        // Phase 1: Distribution
        DigitsMapperAction dist_action(
            m_reader,
            buckets,
            radix_to_d_minus_1,
            m_d,
            m_reader.radix(),
            current_offset,
            chunk_to_process,
            effective_threads,
            partition_size_bits,
            recommended_bucket_capacity
        );
        parallelizer_default.run_in_parallel(dist_action, 0, effective_threads);

        // Phase 2: Application
        ValuesReducerAction app_action(
            seen_strings,
            buckets,
            found_strings_count,
            effective_threads
        );
        parallelizer_default.run_in_parallel(app_action, 0, effective_threads);

        current_offset += chunk_to_process;
        digits_since_last_report += chunk_to_process;

        if (digits_since_last_report >= 1000000000){
            digits_since_last_report = 0;
            auto current_time = std::chrono::high_resolution_clock::now();
            double current_cpu = get_cpu_time();
            std::chrono::duration<double> elapsed = current_time - start_time;
            Console::println(
                "Progress: " + StringTools::tostr(found_strings_count.load(std::memory_order_relaxed), StringTools::COMMAS) +
                " / " + StringTools::tostr(total_strings, StringTools::COMMAS) +
                " strings found. Digits Scanned: " + StringTools::tostr(current_offset, StringTools::COMMAS) +
                ". Time elapsed: " + format_times(elapsed.count(), current_cpu - start_cpu)
            );
        }
    }

    if (effective_threads > 1 && found_strings_count.load(std::memory_order_relaxed) == total_strings) {
      // This is bad: Somehow we found all the strings during the multi-threaded bitvector phase,
      // and that phase was not built to keep track of which string appeared last.  Alert the user.
      Console::println("\n\nSomething went wrong: Found all d-digit strings before keeping careful track of which came last.");
      Console::println("Correct answer UNKNOWN but less than " + StringTools::tostr(current_offset, StringTools::COMMAS));
      return;
    }
    
    // Scan phase 2: Use a (mutex-guarded) hash map to record appearances of the few strings not seen in phase 1.
    std::unordered_map<uiL_t, std::vector<uiL_t>> missing_strings_map;
    std::mutex map_mutex;
    
    // Bloom Filter setup
    const uiL_t BLOOM_BITS_LOG2 = 20;
    const uiL_t BLOOM_SIZE = 1ULL << BLOOM_BITS_LOG2;
    const uiL_t BLOOM_MASK = BLOOM_SIZE - 1;
    std::vector<uint64_t> bloom_filter((BLOOM_SIZE + 63) / 64, 0);

    if (found_strings_count.load(std::memory_order_relaxed) < total_strings) {
        // Build the map of missing strings
        // This could be parallelized too, but it seems so fast that it's not worth it.
        for (size_t i = 0; i < seen_strings.size(); ++i) {
            uint64_t word = seen_strings[i];
            if (word == ~0ULL) continue;

            for (int bit = 0; bit < 64; ++bit) {
                if (!((word >> bit) & 1ULL)) {
                    uiL_t string_val = (uiL_t)i * 64 + bit;
                    if (string_val < total_strings) {
                        missing_strings_map[string_val] = std::vector<uiL_t>();
                        
                        // Populate Bloom Filter
                        uiL_t idx = string_val & BLOOM_MASK;
                        bloom_filter[idx / 64] |= (1ULL << (idx % 64));
                    }
                }
            }
        }
        
        auto current_time = std::chrono::high_resolution_clock::now();
        double current_cpu = get_cpu_time();
        std::chrono::duration<double> elapsed = current_time - start_time;
        Console::println("Map construction complete. Time: " + format_times(elapsed.count(), current_cpu - start_cpu));

        std::atomic<uiL_t> map_unfound_count(missing_strings_map.size());
        Console::println("Processing " + StringTools::tostr((uiL_t)missing_strings_map.size()) + " remaining strings.\n");

        while (map_unfound_count.load(std::memory_order_relaxed) > 0 && current_offset < limit) {
             uiL_t chunk_to_process = std::min((uiL_t)MAX_PARALLEL_CHUNK_SIZE, limit - current_offset); 
             
             DigitMapScanAction action(
                m_reader,
                missing_strings_map,
                bloom_filter,
                BLOOM_MASK,
                map_mutex,
                map_unfound_count,
                radix_to_d_minus_1,
                m_d,
                m_reader.radix(),
                current_offset,
                chunk_to_process,
                effective_threads
             );
             parallelizer_default.run_in_parallel(action, 0, effective_threads);
             current_offset += chunk_to_process;
             digits_since_last_report += chunk_to_process;

            if (digits_since_last_report >= 1000000000){
                digits_since_last_report = 0;
                auto current_time = std::chrono::high_resolution_clock::now();
                double current_cpu = get_cpu_time();
                std::chrono::duration<double> elapsed = current_time - start_time;
                Console::println(
                    "Progress: " + StringTools::tostr(total_strings - map_unfound_count.load(std::memory_order_relaxed), StringTools::COMMAS) +
                    " / " + StringTools::tostr(total_strings, StringTools::COMMAS) +
                    " strings found. Digits Scanned: " + StringTools::tostr(current_offset, StringTools::COMMAS) +
                    ". Time elapsed: " + format_times(elapsed.count(), current_cpu - start_cpu)
                );
            }
        }
        
        // Update found count for final report
        found_strings_count.store(total_strings - map_unfound_count.load(std::memory_order_acquire), std::memory_order_relaxed);
    }

    // Determine the true last string from the map results
    if (!missing_strings_map.empty()) {
        uiL_t max_first_pos = 0;
        uiL_t winner_string = 0;
        
        // For each string found in the map phase, its first occurrence is a candidate "last d-string"
        for (const auto& pair : missing_strings_map) {
             if (!pair.second.empty()) {
                 uiL_t first_pos = pair.second[0];
                 for (size_t i = 1; i < pair.second.size(); ++i) {
                     if (pair.second[i] < first_pos) {
                         first_pos = pair.second[i];
                     }
                 }
                 
                 if (first_pos > max_first_pos) {
                     max_first_pos = first_pos;
                     winner_string = pair.first;
                 }
             }
        }
        
        if (max_first_pos > 0) {
            last_found_digit_pos.store(max_first_pos, std::memory_order_relaxed);
            last_found_d_string.store(winner_string, std::memory_order_relaxed);
        }
    }


    Console::println("\nSearch Complete.");

    uiL_t found = found_strings_count.load(std::memory_order_relaxed);
    if (found == total_strings) {
        Console::println("All " + StringTools::tostr(total_strings, StringTools::COMMAS) + " d-digit strings found!");
        Console::println("The last unique d-digit string (" + StringTools::tostr_width(last_found_d_string.load(std::memory_order_relaxed), m_d) + ") was found at digit position: " + StringTools::tostr(last_found_digit_pos.load(std::memory_order_relaxed), StringTools::COMMAS));
    } else {
        Console::println("Only " + StringTools::tostr(found, StringTools::COMMAS) + " out of " + StringTools::tostr(total_strings, StringTools::COMMAS) + " d-digit strings were found.");
        Console::println("This is " + StringTools::tostr((upL_t)(found * 100 / total_strings)) + "% of all possible strings.");
        Console::println("Digits processed: " + StringTools::tostr(current_offset, StringTools::COMMAS));
        if (total_strings < found + 20) {
            Console::println("The digit strings that haven't appeared yet are:");
            for (const auto& pair : missing_strings_map) {
                if (pair.second.empty()) {
                    Console::println(StringTools::tostr_width(pair.first, m_d));
                }
            }
        }
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double end_cpu = get_cpu_time();
    std::chrono::duration<double> elapsed = end_time - start_time;
    Console::println("\nTotal execution time: " + format_times(elapsed.count(), end_cpu - start_cpu));
}

} // namespace DigitViewer2
