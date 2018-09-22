/**
 * string_utility.h
 */

#pragma once

#include <algorithm>
#include <sstream>
#include <string>
#include <vector>

namespace cmudb {
class StringUtility {
public:
  // trim from start (in place)
  static inline void LTrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(),
                                    [](int ch) { return !std::isspace(ch); }));
  }

  // trim from end (in place)
  static inline void RTrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(),
                         [](int ch) { return !std::isspace(ch); })
                .base(),
            s.end());
  }

  // trim from both sides (in place)
  static inline void Trim(std::string &s) {
    LTrim(s);
    RTrim(s);
  }

  static std::vector<std::string> Split(const std::string &s, char delim) {
    std::stringstream ss;
    std::vector<std::string> elems;
    std::string item;

    ss.str(s);
    while (std::getline(ss, item, delim)) {
      Trim(item);
      elems.push_back(item);
    }

    return elems;
  }
};
} // namespace cmudb
