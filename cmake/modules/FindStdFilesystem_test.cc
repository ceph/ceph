#if __has_include(<filesystem>)
#include <filesystem>
namespace fs = std::filesystem;
#elif __has_include(<experimental/filesystem>)
#include <experimental/filesystem>
namespace fs = std::experimental::filesystem;
#else
#error std::filesystem not available!
#endif

int main() {
    fs::create_directory("sandbox");
    fs::remove_all("sandbox");
}
