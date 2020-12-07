#include <string>

std::string CreateIthKey(int i) {
  return "key" + std::to_string(i);
}
std::string CreateIthValue(int i) {
  return "value" + std::to_string(i);
}
