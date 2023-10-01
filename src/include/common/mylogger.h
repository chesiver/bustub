#include <fmt/chrono.h>
#include <fmt/color.h>
#include <fmt/std.h>

#include <cstdio>

namespace bustub {

#ifndef ThreadMap_H
#define ThreadMap_H

class ThreadMap {
 public:
  void AddThreadId(std::thread::id thread_id) {
    m_.lock();
    thread_map_[thread_id] = count_++;
    m_.unlock();
  }
  auto GetMappedThreadId(std::thread::id thread_id) -> int {
    m_.lock();
    int id = thread_map_[thread_id];
    m_.unlock();
    return id;
  }

 private:
  int count_ = 0;
  std::mutex m_;
  std::unordered_map<std::thread::id, int> thread_map_;
};
#endif

inline ThreadMap global_thread_map;

inline std::ofstream output_stream;

inline FILE *output_file;

#undef LOG_DEBUG
#define LOG_DEBUG(...)                                              \
  {                                                                 \
    auto out = fmt::memory_buffer();                                \
    OutputLogHeaderV2(out, __SHORT_FILE__, __LINE__, __FUNCTION__); \
    fmt::format_to(std::back_inserter(out), __VA_ARGS__);           \
    fmt::format_to(std::back_inserter(out), "\n");                  \
    fmt::print(output_file, fmt::to_string(out));                   \
  }

inline void OutputLogHeaderV2(fmt::memory_buffer &out, const char *file, int line, const char *func) {
  std::time_t now = std::time(nullptr);
  std::thread::id thread_id = std::this_thread::get_id();
  int id = global_thread_map.GetMappedThreadId(thread_id);
  // size_t id = std::hash<std::thread::id>()(thread_id);
  std::vector<fmt::color> colors = {fmt::color::purple,     fmt::color::yellow,     fmt::color::red,
                                    fmt::color::blue,       fmt::color::green,      fmt::color::magenta,
                                    fmt::color::light_blue, fmt::color::light_green};
  fmt::color c = colors[id % colors.size()];
  fmt::format_to(std::back_inserter(out), fg(c), "id: {} - {:%H:%M:%S} [{}:{}] - ", id, fmt::localtime(now), line,
                 func);
}

}  // namespace bustub