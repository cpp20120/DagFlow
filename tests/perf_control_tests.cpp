// Linux perf's acknowledgement protocol accepts both newline and newline/NUL.
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

int main(int argc, char** argv) {
  if (argc != 2) return 1;
  for (const bool nul : {false, true}) {
    int control[2], ack[2];
    if (pipe(control) || pipe(ack)) return 1;
    const auto child = fork();
    if (child < 0) return 1;
    if (child == 0) {
      close(control[0]);
      close(ack[1]);
      const auto cw = std::to_string(control[1]);
      const auto ar = std::to_string(ack[0]);
      execl(argv[1], argv[1], "--json", "--tasks", "37", "--workers", "1",
            "--scenario", "external-contention", "--repeats", "2", "--warmup", "3",
            "--warmup-ms", "0", "--verify", "exact", "--perf-control-fd", cw.c_str(),
            "--perf-ack-fd", ar.c_str(), static_cast<char*>(nullptr));
      _exit(127);
    }
    close(control[1]);
    close(ack[0]);
    // CTest also bounds the whole process tree; never hang on a missing command.
    alarm(15);
    std::string commands;
    char c;
    while (true) {
      const auto count = read(control[0], &c, 1);
      if (count < 0 && errno == EINTR) continue;
      if (count != 1) break;
      commands += c;
      if (c == '\n') {
        const std::string reply = nul ? std::string("ack\n\0", 5) : "ack\n";
        if (write(ack[1], reply.data(), reply.size()) != static_cast<ssize_t>(reply.size()))
          return 1;
      }
    }
    close(control[0]);
    close(ack[1]);
    int status = 0;
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status) ||
        commands != "enable\ndisable\nenable\ndisable\n") {
      std::fprintf(stderr, "Invalid perf control sequence: %s\n", commands.c_str());
      return 1;
    }
    alarm(0);
  }
}
