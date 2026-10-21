#include <cstdio>
#include <cstring>

int main(int argc, char** argv) {
  if (argc == 1) return 0;
  if (argc == 4 && std::strcmp(argv[1], "4") == 0 &&
      std::strcmp(argv[2], "4096") == 0 && std::strcmp(argv[3], "9") == 0)
    return 0;
  if (argc == 5 && std::strcmp(argv[1], "--label") == 0 &&
      std::strcmp(argv[2], "two words") == 0 &&
      std::strcmp(argv[3], "--workers") == 0 && std::strcmp(argv[4], "2") == 0)
    return 0;
  std::fprintf(stderr, "Unexpected argv (%d arguments)\n", argc - 1);
  for (int i = 1; i < argc; ++i) std::fprintf(stderr, "[%s]\n", argv[i]);
  return 1;
}
