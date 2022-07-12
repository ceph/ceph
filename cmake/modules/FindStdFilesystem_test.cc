#include <filesystem>

namespace fs = std::filesystem;

int main() {
    fs::create_directory("sandbox");
    fs::remove_all("sandbox");
}
