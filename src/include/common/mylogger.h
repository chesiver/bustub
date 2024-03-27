#pragma once

#include <fmt/chrono.h>
#include <fmt/color.h>
#include <fmt/std.h>
#include <unordered_map>
#include <vector>

#include <cstdio>

namespace bustub {

class ThreadMap {
 public:
  auto GetMappedThreadId(std::thread::id thread_id) -> int {
    std::scoped_lock lock{m_};
    if (thread_map_.find(thread_id) == thread_map_.end()) {
      thread_map_[thread_id] = count_++;
    }
    return thread_map_[thread_id];
  }

 private:
  int count_ = 0;
  std::mutex m_;
  std::unordered_map<std::thread::id, int> thread_map_;
};

inline ThreadMap global_thread_map;

inline std::ofstream output_stream;

inline FILE *output_file;

#undef MY_LOG_DEBUG
#define MY_LOG_DEBUG(...)                                           \
  {                                                                 \
    auto out = fmt::memory_buffer();                                \
    OutputLogHeaderV2(out, __SHORT_FILE__, __LINE__, __FUNCTION__); \
    fmt::format_to(std::back_inserter(out), __VA_ARGS__);           \
    fmt::format_to(std::back_inserter(out), "\n");                  \
    fmt::print(fmt::to_string(out));                                \
  }

inline void OutputLogHeaderV2(fmt::memory_buffer &out, const char *file, int line, const char *func) {
  std::time_t now = std::time(nullptr);
  std::thread::id thread_id = std::this_thread::get_id();
  int id = global_thread_map.GetMappedThreadId(thread_id);
  std::vector<fmt::color> colors = {fmt::color::purple,     fmt::color::yellow,      fmt::color::red,
                                    fmt::color::blue,       fmt::color::green,       fmt::color::magenta,
                                    fmt::color::light_blue, fmt::color::light_green, fmt::color::aqua};
  fmt::color c = colors[id % colors.size()];
  fmt::format_to(std::back_inserter(out), fg(c), "id: {} - {:%H:%M:%S} [{}:{}] - ", id, fmt::localtime(now), line,
                 func);
}

}  // namespace bustub
