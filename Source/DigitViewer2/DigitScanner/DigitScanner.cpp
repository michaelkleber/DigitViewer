/* DigitScanner.cpp
 * 
 * Author           : Michael Kleber
 * Date Created     : 01/15/2026
 * Last Modified    : 01/15/2026
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

class DigitBitvectorScanAction : public ymp::BasicAction {
public:
    DigitBitvectorScanAction(
        BasicDigitReader& reader,
        std::vector<std::atomic<uint64_t>>& seen_strings_atomic,
        std::atomic<uiL_t>& found_strings_count,
        std::atomic<uiL_t>& last_found_digit_pos,
        std::atomic<uiL_t>& last_found_d_string,
        uiL_t radix_to_d_minus_1,
        upL_t digits,
        char radix,
        uiL_t current_stream_offset,
        uiL_t chunk_to_process,
        upL_t num_threads
    )
        : m_reader(reader)
        , m_seen_strings_atomic(seen_strings_atomic)
        , m_found_strings_count(found_strings_count)
        , m_last_found_digit_pos(last_found_digit_pos)
        , m_last_found_d_string(last_found_d_string)
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
        // these only matter when num_threads==1, but it's faster to just track them unconditionally
        uiL_t thread_last_found_digit_pos = 0;
        uiL_t thread_last_found_d_string = 0;

        // Each thread gets its own buffer
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

        // Ring buffer for lookahead values, so that we can prefetch the right part of the bitvector.
	    // From empirical testing, PREFETCH_DIST 64 and 96 seem equally good, 32 and 128 are both worse.
	    // This is balancing the CPU inner loop speed, the memory prefetch speed, and the L1 cache size,
	    // so different prefetch distances will be optimal for different hardware.  If you're tuning for
	    // your machine, I would recommend using a PREFETCH_DIST value that makes d=9 fastest.
        const int PREFETCH_DIST = 96;
        uiL_t lookahead_values[PREFETCH_DIST];
        uiL_t current_lookahead_hash = thread_d_string_value;
        upL_t num_digits = end_offset - start_offset;

        // Phase 1: Pre-fill the ring buffer
        for (upL_t i = 0; i < PREFETCH_DIST && i < num_digits; ++i) {
            char digit = dec_digits[i];
            current_lookahead_hash = (current_lookahead_hash % m_radix_to_d_minus_1) * m_radix + (digit - '0');
            lookahead_values[i] = current_lookahead_hash;
        }

        // Phase 2: Main processing loop
        for (upL_t i = 0; i < num_digits; ++i) {
            // Retrieve pre-calculated value
            uiL_t val = lookahead_values[i % PREFETCH_DIST];

            // Process current value, checking whether the corresponding bit in the (shared) bitvector
            // is already 1, and if not, trying to flip it to 1 atomically.  Check first because most of
            // the time the bit is already 1 and the write is a no-op with nonzero cost.
            uiL_t idx = val / 64;
            uint64_t mask = 1ULL << (val % 64);
			if (!(m_seen_strings_atomic[idx].load(std::memory_order_relaxed) & mask)) {
                uint64_t old_val = m_seen_strings_atomic[idx].fetch_or(mask, std::memory_order_relaxed);
                if ((old_val & mask) == 0) {
                    m_found_strings_count.fetch_add(1, std::memory_order_relaxed);
                    // these only matter when num_threads==1, but it's faster to just track them unconditionally
                    thread_last_found_digit_pos = start_offset + i + 1;
                    thread_last_found_d_string = val;
                }
            }

            // Calculate and prefetch future value.  This line of memory should be in L1 cache by the time the
	        // inner loop next circles around to this location in the ring buffer.
            upL_t future_idx = i + PREFETCH_DIST;
            if (future_idx < num_digits) {
                char next_digit = dec_digits[future_idx];
                current_lookahead_hash = (current_lookahead_hash % m_radix_to_d_minus_1) * m_radix + (next_digit - '0');
                lookahead_values[i % PREFETCH_DIST] = current_lookahead_hash;
                
                uiL_t p_idx = current_lookahead_hash / 64;
                PREFETCH(&m_seen_strings_atomic[p_idx]);
            }
        }
        if (m_num_threads == 1) {
            m_last_found_digit_pos.store(thread_last_found_digit_pos, std::memory_order_relaxed);
            m_last_found_d_string.store(thread_last_found_d_string, std::memory_order_relaxed);
        }
    }

private:
    BasicDigitReader& m_reader;
    std::vector<std::atomic<uint64_t>>& m_seen_strings_atomic;
    std::atomic<uiL_t>& m_found_strings_count;
    std::atomic<uiL_t>& m_last_found_digit_pos;
    std::atomic<uiL_t>& m_last_found_d_string;
    uiL_t m_radix_to_d_minus_1;
    upL_t m_d;
    char m_radix;
    uiL_t m_current_stream_offset;
    uiL_t m_chunk_to_process;
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
    std::vector<std::atomic<uint64_t>> seen_strings_atomic(num_atomic_words);
    for (auto& word : seen_strings_atomic) {
        word.store(0, std::memory_order_relaxed);
    }
    
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

    // Start scanning from the first possible d-digit string.
    // The first string ends at index d-1 (0-based).
    // The parallel action will look back d-1 digits to seed the sliding window correctly.
    current_offset = m_d - 1;
    
    // We haven't processed any digits yet.
    digits_since_last_report = 0;

    const upL_t MAX_PARALLEL_CHUNK_SIZE = 100000000;
    const upL_t MIN_PARALLEL_CHUNK_SIZE = 1000000;
    const upL_t SEQUENTIAL_BLOCK_SIZE = 1000000;
    upL_t tds = Environment::GetLogicalProcessors();
    
    // If the total number of strings is small enough that they might all appear in a single chunk,
    // run sequentially to ensure we correctly identify the last string.
    // If there are enough strings that we run on multiple threads, then we will switch out of
    // parallel execution when unfound_count < 10000, checked inside the loop below.
    upL_t effective_threads = tds;
    if (total_strings < MAX_PARALLEL_CHUNK_SIZE) {
        effective_threads = 1;
        Console::println("Total strings (" + StringTools::tostr(total_strings) + ") is small. Forcing sequential mode for correctness.");
    } else {
        Console::println("Using " + StringTools::tostr(effective_threads) + " threads for parallel processing.");
    }

    // Scan phase 1: Use a bitvector (of atomics) to record which of the 10^d strings have been seen.
    while (found_strings_count.load(std::memory_order_acquire) < total_strings && current_offset < limit) {
        uiL_t found_count = found_strings_count.load(std::memory_order_relaxed);
        uiL_t unfound_count = total_strings - found_count;

        if (effective_threads > 1 && unfound_count < 10000){
            // With this few strings left to search for, it is faster to switch to the Map-based scan	    
            auto current_time = std::chrono::high_resolution_clock::now();
            double current_cpu = get_cpu_time();
            std::chrono::duration<double> elapsed = current_time - start_time;
            Console::println("\n" + StringTools::tostr(unfound_count) +
			     " strings remaining. Switching to map-based parallel mode. Time: " +
			     format_times(elapsed.count(), current_cpu - start_cpu));
            break;
        }

        uiL_t chunk_to_process = std::min((uiL_t)MAX_PARALLEL_CHUNK_SIZE, limit - current_offset);

        DigitBitvectorScanAction action(
            m_reader,
            seen_strings_atomic,
            found_strings_count,
            last_found_digit_pos,
            last_found_d_string,
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
    const uiL_t BLOOM_BITS_LOG2 = 18;
    const uiL_t BLOOM_SIZE = 1ULL << BLOOM_BITS_LOG2;
    const uiL_t BLOOM_MASK = BLOOM_SIZE - 1;
    std::vector<uint64_t> bloom_filter((BLOOM_SIZE + 63) / 64, 0);

    if (found_strings_count.load(std::memory_order_relaxed) < total_strings) {
        // Build the map of missing strings
        // This could be parallelized too, but it seems so fast that it's not worth it.
        for (size_t i = 0; i < seen_strings_atomic.size(); ++i) {
            uint64_t word = seen_strings_atomic[i].load(std::memory_order_relaxed);
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
