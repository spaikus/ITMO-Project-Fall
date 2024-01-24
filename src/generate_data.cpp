#include <filesystem>
#include <iostream>
#include <string>

#include "rows.hpp"

#include <generators/generate_data.hpp>

int main() {
  using Row = RowFew;
  const auto &kRowGenSchema = kRowFewGenSchema;
  const std::string static_folder = "static/";

  std::filesystem::create_directory(static_folder);
  
  {
    constexpr size_t gbs = 16;
    constexpr size_t mem_size = gbs * 1_GiB;
    constexpr size_t rows = mem_size / sizeof(Row);
    constexpr size_t batch_size = 256_MiB;

    PARQUET_THROW_NOT_OK(generators::GenerateParquet(
        static_folder + "row_" + std::to_string(gbs) + "gib.parquet",
        kRowGenSchema, rows, batch_size / sizeof(Row)));
  }

  return 0;
}
