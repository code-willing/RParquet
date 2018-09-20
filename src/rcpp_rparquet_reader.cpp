#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/api/reader.h>
#include <parquet/exception.h>
#include <Rcpp.h>
#include <unordered_map>
#include <arrow/util/thread-pool.h>
using namespace Rcpp;
namespace RParquet {

namespace Time_Constants {
  static const uint64_t ticks_per_second = 1000000000UL; // nanosecond resolution
  static const uint64_t ticks_per_mili   = 1000000UL; // nanosecond resolution
  static const uint64_t ticks_per_micro  = 1000UL; // nanosecond resolution
}

class RParquet_Reader
{
public:
  RParquet_Reader(std::string filename,
                  IntegerVector selected_col,
                  LogicalVector filter,
                  int read_row_size,
                  int threads,
                  int verbose
                  ) :
    m_filename(filename),
    m_col_idx(as<std::vector<int>>(selected_col)),
    m_row_filter(as<std::vector<int>>(filter)),
    m_read_row_size(read_row_size),
    m_threads(threads),
    m_verbose(verbose)
    {
    }

  ~RParquet_Reader(){}

  void init() {
    if (m_read_row_size < 1) {
      stop("Read row size should greater than 0.");
    }
    m_row_selected_size = std::count(m_row_filter.begin(), m_row_filter.end(), 1);
    if (m_row_selected_size == 0) {
      stop("All rows are skipped by the filter.");
    }
    std::shared_ptr<arrow::io::ReadableFile> infile;
    PARQUET_THROW_NOT_OK(arrow::io::ReadableFile::Open(m_filename, arrow::default_memory_pool(), &infile));
    PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &m_reader));
    if(m_threads > 1) {
     PARQUET_THROW_NOT_OK(arrow::SetCpuThreadPoolCapacity(m_threads));
     m_reader->set_use_threads(true);
    }
    PARQUET_THROW_NOT_OK(m_reader->ReadTable(&m_table));

    m_rows = m_table->num_rows();
    m_cols = m_table->num_columns();
    m_schema = m_table->schema();
    m_row_groups = m_reader->num_row_groups();
    m_rows_per_group = m_read_row_size > m_rows ? (m_rows / m_row_groups) : m_read_row_size;

    if (m_row_selected_size == 1 && m_row_filter.size() == 1) {
      m_row_selected_size = 0;
    } else if (m_row_selected_size != 0 && m_row_filter.size() < m_rows) {
      m_row_filter.resize(m_rows, 0);
    }
    if (m_col_idx.size() == 1 && m_col_idx[0] == -1) {
      int i = 1;
      while(i <= m_cols) {
        m_col_idx_set.insert(i++);
      }
    } else {
      std::copy_if(m_col_idx.begin(), m_col_idx.end(), std::inserter(m_col_idx_set, m_col_idx_set.begin()),
      [&](const int i) { return (i > 0 && i <= m_cols); });
    }
    if (m_col_idx_set.size() == 0) {
          stop("ERROR:No valid column has been selected\n");
    }

    m_fields = m_schema->fields();
    for (auto & f: m_fields) {
      m_col_names.push_back(f->name());
      m_col_types.push_back(f->type()->id());
      m_col_types_names.push_back(f->type()->name());
    }

    if (m_verbose == 1) {
      Rcout << "\n";
      Rcout << "TOTAL ROWS:" << m_rows<<"\n";
      Rcout << "TOTAL COLS:" << m_cols<<"\n";
      Rcout << "SELECTED COLUMNS:\n";
      for (auto &e : m_col_idx_set) {
        Rcout << "[id:"<< e << ", name:" << m_col_names[e-1] <<", type:" << m_col_types[e-1] << "]" <<"\n";
      }
      Rcout << "ROW GROUPS:" << m_row_groups<<"\n";
      Rcout << "ROWS/GROUP:" << m_rows / m_row_groups <<"\n";
      Rcout << "EACH READ SIZE:" << m_rows_per_group <<"\n";
      Rcout << "ROW FILTER SIZE:" << m_row_selected_size <<"\n";
      Rcout << "SCHEMA:\n" << m_schema->ToString()<<"\n";
    }
  }

  SEXP create_df(){
    int selected_col_size = m_col_idx_set.size();
    List col_list(selected_col_size);
    List col_name(selected_col_size);
      int index = 0;
      for (auto &col_idx: m_col_idx_set) {
        col_name[index] = m_col_names[col_idx - 1];
        col_list[index] = read_col(col_idx - 1);
        index++;
      }
      col_list.attr("names") = col_name;
      col_list.attr("row.names") = IntegerVector::create(NA_INTEGER, -m_rows);
    return col_list;
  }

  SEXP read_col(int col_idx) {
    int capacity{m_row_selected_size == 0 ? m_rows : m_row_selected_size};
    switch(m_col_types[col_idx]) {
      case arrow::Type::type::INT64: {
        NumericVector nvec = NumericVector(capacity);
        const int64_t* data;
        read_row_group<arrow::Int64Array>(NA_REAL, data, nvec, col_idx,
            [](auto arrow_ary, auto i, int64_t idx, const auto* raw_data_ary, auto &rcpp_vec)
            {std::memcpy(&(rcpp_vec[idx]), &(raw_data_ary[i]), sizeof(double));});
        nvec.attr("class") = "integer64";
        return nvec;
       }
      case arrow::Type::type::DOUBLE: {
        NumericVector nvec = NumericVector(capacity);
        const double* data;
        read_row_group<arrow::DoubleArray>(NA_REAL, data, nvec, col_idx,
            [](auto arrow_ary, auto i, int64_t idx, const auto* raw_data_ary, auto &rcpp_vec)
            { rcpp_vec[idx] = raw_data_ary[i];});
        return nvec;
      }
      case arrow::Type::type::INT32: {
        IntegerVector ivec = IntegerVector(capacity);
        const int32_t* data;
        read_row_group<arrow::Int32Array>(NA_INTEGER, data, ivec, col_idx,
            [](auto arrow_ary, auto i, int64_t idx, const auto* raw_data_ary, auto &rcpp_vec)
            { rcpp_vec[idx] = raw_data_ary[i];});
        return ivec;
      }
      case arrow::Type::type::BOOL: {
        LogicalVector lvec = LogicalVector(capacity);
        const uint8_t* data;
        int64_t offset = 0;
        int64_t filter_offset = 0;
        for (auto group_idx = 0; group_idx < m_row_groups; ++group_idx) {
          std::shared_ptr<arrow::Array> array;
          PARQUET_THROW_NOT_OK(m_reader->RowGroup(group_idx)->Column(col_idx)->Read(&array));
          std::shared_ptr<arrow::BooleanArray> arrow_array = std::static_pointer_cast<arrow::BooleanArray>(array);
          auto row_len = arrow_array->length();
          if (m_row_selected_size == 0) {
            for (auto i = 0; i < row_len; ++i) {
              if (array->IsNull(i)) {
                lvec[i+ offset] = NA_LOGICAL;
              } else {
                lvec[i + offset] = arrow_array->Value(i);
              }
             }
           } else {
           for (auto i = 0; i < row_len; ++i) {
              if (m_row_filter[i + offset]) {
                if (arrow_array->IsNull(i)) {
                  lvec[filter_offset++] = NA_LOGICAL;
                } else {
                  lvec[filter_offset++] = arrow_array->Value(i);
                }
              }
            }
          }
          offset += row_len;
        }
        return lvec;
      }
      case arrow::Type::type::BINARY:
      case arrow::Type::type::STRING: {
        CharacterVector cvec = CharacterVector(capacity);
        const std::string* data;
        int64_t offset = 0;
        int64_t filter_offset = 0;
        for (auto group_idx = 0; group_idx < m_row_groups; ++group_idx) {
          std::shared_ptr<arrow::Array> array;
          PARQUET_THROW_NOT_OK(m_reader->RowGroup(group_idx)->Column(col_idx)->Read(&array));
          std::shared_ptr<arrow::StringArray> arrow_array = std::static_pointer_cast<arrow::StringArray>(array);
          auto row_len = arrow_array->length();
          if (m_row_selected_size == 0) {
            for (auto i = 0; i < row_len; ++i) {
              if (array->IsNull(i)) {
                cvec[i+ offset] = NA_STRING;
              } else {
                cvec[i + offset] = arrow_array->GetString(i);
              }
             }
          } else {
            for (auto i = 0; i < row_len; ++i) {
              if (m_row_filter[i + offset]) {
                if (arrow_array->IsNull(i)) {
                  cvec[filter_offset++] = NA_STRING;
                } else {
                  cvec[filter_offset++] = arrow_array->GetString(i);
                }
              }
            }
          }
          offset += row_len;
        }
        return cvec;
      }
      case arrow::Type::type::TIMESTAMP: {
        NumericVector nvec = NumericVector(capacity);
        const int64_t* data;
        read_row_group<arrow::TimestampArray>(NA_REAL, data, nvec, col_idx,
            [](auto arrow_ary, auto i, int64_t idx, const auto* raw_data_ary, auto &rcpp_vec){
        int64_t nano_t = raw_data_ary[i];
        switch (std::dynamic_pointer_cast<arrow::TimestampType>(arrow_ary->type())->unit()) { // ->timezone()
          case arrow::TimeUnit::SECOND:
            nano_t = raw_data_ary[i]*Time_Constants::ticks_per_second;
            break;
          case arrow::TimeUnit::MILLI:
            nano_t = raw_data_ary[i]*Time_Constants::ticks_per_mili;
            break;
          case arrow::TimeUnit::MICRO:
            nano_t = raw_data_ary[i]*Time_Constants::ticks_per_micro;
            break;
        }
        std::memcpy(&(rcpp_vec[idx]), &(nano_t), sizeof(double));});
        Rcpp::CharacterVector cl = Rcpp::CharacterVector::create("nanotime");
        cl.attr("package") = "nanotime";
        nvec.attr(".S3Class") = "integer64";
        nvec.attr("class") = cl;
        SET_S4_OBJECT(nvec);
        return Rcpp::S4(nvec);
      }
      default: {
         stop("Unknown column, name:%s, type: %s",
             m_col_names[col_idx], m_col_types_names[col_idx]);
      }
    }
  }

  template <typename ArrowArrayType, typename NAType, typename DataType, typename VectorType, typename FuncType>
  void read_row_group(NAType NA, const DataType* data, VectorType& rcpp_vec, int col_idx, FuncType convert_to_rvector) {
    int64_t offset = 0;
    int64_t filter_offset = 0;
    for (auto group_idx = 0; group_idx < m_row_groups; ++group_idx) {
      std::shared_ptr<arrow::Array> array;
      PARQUET_THROW_NOT_OK(m_reader->RowGroup(group_idx)->Column(col_idx)->Read(&array));
      std::shared_ptr<ArrowArrayType> arrow_array = std::static_pointer_cast<ArrowArrayType>(array);
      auto row_len = arrow_array->length();
      data = arrow_array->raw_values();
      if (m_row_selected_size == 0) {
        for (auto i = 0; i < row_len; ++i) {
            if (arrow_array->IsNull(i)) {
              rcpp_vec[i+ offset] = NA;
            } else {
              int64_t idx =  i + offset;
              convert_to_rvector(arrow_array, i, idx, data, rcpp_vec);
            }
          }
        } else {
          for (auto i = 0; i < row_len; ++i) {
            if (m_row_filter[i + offset]) {
              if (arrow_array->IsNull(i)) {
                rcpp_vec[filter_offset++] = NA;
              } else {
                convert_to_rvector(arrow_array, i, filter_offset, data, rcpp_vec);
                filter_offset++;
              }
            }
          }
        }
      offset += row_len;
    }
  }

private:
  int m_row_selected_size;
  int m_read_row_size;
  int m_verbose;
  int m_rows;
  int m_cols;
  int m_row_groups;
  int m_rows_per_group;
  int m_threads;
  std::string                                 m_timezone;
  std::string                                 m_filename;
  std::vector<int>                            m_col_idx;
  std::vector<int>                            m_row_filter;
  std::unique_ptr<parquet::arrow::FileReader> m_reader;
  std::vector<std::shared_ptr<arrow::Field>>  m_fields;
  std::vector<int16_t>                        m_def_level;
  std::vector<int16_t>                        m_repeat_level;
  std::vector<uint8_t>                        m_valid_bits;
  std::set<int>                               m_col_idx_set;
  std::vector<std::string>                    m_col_names;
  std::vector<arrow::Type::type>              m_col_types;
  std::vector<std::string>                    m_col_types_names;
  std::shared_ptr<arrow::Schema>              m_schema;
  std::shared_ptr<arrow::Table>               m_table;

};
} // namespace RParquet

// [[Rcpp::export]]
List read_parquet(std::string filename, IntegerVector selected_col, LogicalVector filter, int read_row_size, int threads, int verbose) {
  RParquet::RParquet_Reader rp_reader(filename, selected_col, filter, read_row_size, threads, verbose);
  rp_reader.init();
  return rp_reader.create_df();
}

// [[Rcpp::export]]
DataFrame read_metadata(std::string filename, bool details = false) {
  const std::string names[6] = {
        "COL_NAME", "COL_PHY_TYPE", "COL_LOG_TYPE",
        "COL_EMPTY_VALUES", "COL_TOTAL_COMPRESSED_SIZE",
        "COL_TOTAL_UNCOMPRESSED_SIZE" };
  std::vector <std::vector<std::string>> column_meta(6);

  DataFrame df = DataFrame::create();
  std::unique_ptr<parquet::ParquetFileReader> parquet_reader = parquet::ParquetFileReader::OpenFile(filename, false);
  std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();
  int num_row_groups = file_metadata->num_row_groups();
  int num_columns = file_metadata->num_columns();

  // File meta information
  CharacterVector fmeta_vec;
  fmeta_vec.push_back("Filename : " + filename);
  fmeta_vec.push_back("Row Groups : " + std::to_string(num_row_groups));
  fmeta_vec.push_back("Total Rows : " + std::to_string(file_metadata->num_rows()));
  fmeta_vec.push_back("Number of Columns : " + std::to_string(num_columns));
  df.push_back(fmeta_vec, "FILE_META");

  // Simple column meta information
  for (int j = 0; j < num_columns; ++j) {
    const parquet::ColumnDescriptor* descr = file_metadata->schema()->Column(j);
    column_meta[0].push_back(descr->name());
    column_meta[1].push_back(TypeToString(descr->physical_type()));
    column_meta[2].push_back(LogicalTypeToString(descr->logical_type()));
  }
  for (int i = 0; i < 3; ++i) {
    CharacterVector rcpp_vec;
    rcpp_vec.assign(column_meta[i].begin(), column_meta[i].end());
    df.push_back(rcpp_vec, names[i]);
  }

  if (details) {
    for (int idx = 0; idx < num_row_groups; ++idx) {
      auto group_metadata = file_metadata->RowGroup(idx);
      for (int col_idx = 0; col_idx < num_columns; ++col_idx) {
        auto column_chunk = group_metadata->ColumnChunk(col_idx);
        std::shared_ptr < parquet::RowGroupStatistics > stats = column_chunk->statistics();
        Rcout << " Column " << col_idx << " ,Values: " << column_chunk->num_values() << "\n";
        Rcout << " Column " << col_idx << " ,uncompsize/compsize " << column_chunk->total_uncompressed_size() << "/"<< column_chunk->total_compressed_size() << "\n";
        if (idx == 0) {
          if (column_chunk->is_stats_set()) {
            Rcout << stats->null_count() << "\n";
            column_meta[3].push_back(std::to_string(stats->null_count()));
          }
          column_meta[4].push_back(std::to_string(column_chunk->total_compressed_size()));
          column_meta[5].push_back(std::to_string(column_chunk->total_uncompressed_size()));
        } else {
          if (column_chunk->is_stats_set()) {
            auto & empty_val = column_meta[3][col_idx];
            empty_val = std::to_string(std::stoi(empty_val) + stats->null_count());
          }
          auto &total_compress =  column_meta[3][col_idx];
          total_compress = std::to_string(std::stoi(total_compress) + column_chunk->total_compressed_size());
          auto &total_uncompress = column_meta[4][col_idx];
          total_uncompress = std::to_string(std::stoi(total_uncompress) + column_chunk->total_uncompressed_size());
        }
      }
    }
    for (int i = 3; i < 6; ++i) {
      CharacterVector rcpp_vec;
      rcpp_vec.assign(column_meta[i].begin(), column_meta[i].end());
      df.push_back(rcpp_vec, names[i]);
    }
 }
  return df;
}

