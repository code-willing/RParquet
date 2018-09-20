#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <Rcpp.h>
#include <unordered_map>

using namespace Rcpp;
using arrow::Type;

namespace RParquet {
class Rparquet_Writer {
public:
  Rparquet_Writer(DataFrame &df,
                  std::string filename,
                  CharacterVector col_types,
                  IntegerVector col_idx,
                  int group_rows,
                  int v_flag):
        m_df(df),
        m_filename(filename),
        m_col_types(as<std::vector<std::string>>(col_types)),
        m_col_idx(as<std::vector<int>>(col_idx)),
        m_rows_per_group(group_rows),
        m_verbose(v_flag) {
  }

  ~Rparquet_Writer() {
  }
  using BufferVector = std::vector<std::shared_ptr<arrow::Buffer>>;
  void init() {
    m_cols = m_df.length();
    m_rows = m_df.nrows();
    m_row_groups = 1;
    m_row_remainder = 0;
    m_col_names = as<std::vector<std::string>>(m_df.names());

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

    if (m_rows > m_rows_per_group) {
      m_row_groups = m_rows / m_rows_per_group;
      m_row_remainder = m_rows % m_rows_per_group;
    } else {
      m_rows_per_group = m_rows;
    }
    if (m_verbose == 1) {
       Rcout << "\n";
       Rcout << "FILE TO WRITE: " << m_filename << "\n";
       Rcout << "TOTAL ROWS:" << m_rows<<"\n";
       Rcout << "TOTAL COLS:" << m_cols<<"\n";
       Rcout << "SELECTED COLUMNS:\n";
       for (auto &e : m_col_idx_set) {
         Rcout << "[id:"<< e << ", name:" << m_col_names[e-1] <<", type:" << m_col_types[e-1] << "]" <<"\n";
       }
       Rcout << "ROW GROUPS:" << m_row_groups<<"\n";
       Rcout << "ROWS/GROUP:" << m_rows_per_group <<"\n";
       Rcout << "ROW REMAINDER:" << m_row_remainder <<"\n";
     }
  }

  void write_parquet() {
    for (const auto & idx : m_col_idx_set) {
      auto index = idx-1;
      auto type = m_parquet_type_map.find(m_col_types[index]);
      if (type == m_parquet_type_map.end()) {
       Rcout << "ERROR:Unknown parquet data type " << m_col_types[index] << "\n";
       return;
      }
      switch(m_parquet_type_map.at(m_col_types[index])) {
      case Type::type::TIMESTAMP: {
        NumericVector nv = m_df[index];
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO), arrow::default_memory_pool());
        std::vector<int64_t> i64vec(nv.size());
        std::vector<bool> is_valid(nv.size(), true);
        for (int i = 0; i < nv.size(); ++i) {
          if( nv[i] == -0) {
            is_valid[i] = false;
          }
          int64_t x = 0;
          memcpy(&x, &nv[i], sizeof(double));
          i64vec[i] = x;
        }
        PARQUET_THROW_NOT_OK(builder.AppendValues(i64vec, is_valid));
        std::shared_ptr<arrow::Array> ary;
        PARQUET_THROW_NOT_OK(builder.Finish(&ary));
        m_table_arrays.push_back(ary);
        m_table_fields.emplace_back(arrow::field(m_col_names[index], arrow::timestamp(arrow::TimeUnit::NANO)));
      }
        break;
      case Type::type::INT32: {
        IntegerVector iv = m_df[index];
        std::vector<int> ivec = as<std::vector<int>>(iv);
        arrow::Int32Builder builder;
        std::vector<bool> is_valid(iv.size(), true);
        for (int i = 0; i < iv.size(); ++i) {
          if (IntegerVector::is_na(iv[i])) {
            is_valid[i] = false;
          }
        }
        PARQUET_THROW_NOT_OK(builder.AppendValues(ivec, is_valid));
        std::shared_ptr<arrow::Array> ary;
        PARQUET_THROW_NOT_OK(builder.Finish(&ary));
        m_table_arrays.push_back(ary);
        m_table_fields.emplace_back(arrow::field(m_col_names[index], arrow::int32()));
      }
        break;
      case Type::type::INT64: {
        NumericVector nv = m_df[index];
        arrow::Int64Builder builder;
        std::vector<int64_t> i64vec(nv.size());
        std::vector<bool> is_valid(nv.size(), true);
        for (int i = 0; i < nv.size(); ++i) {
          if( nv[i] < 0 && nv[i] == -0) {
            is_valid[i] = false;
          }
          int64_t x = 0;
          memcpy(&x, &nv[i], sizeof(double));
          i64vec[i] = x;
        }
        PARQUET_THROW_NOT_OK(builder.AppendValues(i64vec, is_valid));
        std::shared_ptr<arrow::Array> ary;
        PARQUET_THROW_NOT_OK(builder.Finish(&ary));
        m_table_arrays.push_back(ary);
        m_table_fields.emplace_back(arrow::field(m_col_names[index], arrow::int64()));
      }
        break;
      case Type::type::DOUBLE: {
        NumericVector nv = m_df[index];
        std::vector<bool> is_valid(nv.size(), true);
        for(int i = 0; i < nv.size(); ++i) {
          if(NumericVector::is_na(nv[i])) {
            is_valid[i] = false;
          }
        }
        std::vector<double> dvec = as<std::vector<double>>(nv);
        arrow::DoubleBuilder builder;
        PARQUET_THROW_NOT_OK(builder.AppendValues(dvec, is_valid));
        std::shared_ptr<arrow::Array> ary;
        PARQUET_THROW_NOT_OK(builder.Finish(&ary));
        m_table_arrays.push_back(ary);
        m_table_fields.emplace_back(arrow::field(m_col_names[index], arrow::float64()));
        break;
      }
      case Type::type::STRING: {
        CharacterVector cv = m_df[index];
        arrow::StringBuilder builder;
        std::shared_ptr<arrow::Array> ary;
        std::vector<std::string> svec = as<std::vector<std::string>>(cv);
        uint8_t is_valid[cv.size()];
        for (int i = 0; i < cv.size(); ++i) {
          if (CharacterVector::is_na(cv[i])) {
            is_valid[i] = 0;
          }else{
            is_valid[i] = 1;
          }
        }
        PARQUET_THROW_NOT_OK(builder.AppendValues(svec, is_valid));
        PARQUET_THROW_NOT_OK(builder.Finish(&ary));
        m_table_arrays.push_back(ary);
        m_table_fields.emplace_back(arrow::field(m_col_names[index],  arrow::utf8()));
      }
        break;
      case Type::type::BOOL: {
        LogicalVector lv = m_df[index];
        arrow::BooleanBuilder builder;
        std::vector<bool> is_valid(lv.size(), true);
        for(int i = 0; i < lv.size(); ++i) {
          if(LogicalVector::is_na(lv[i])) {
           is_valid[i] = false;
          }
        }
        std::vector<bool> bvec= as<std::vector<bool>>(lv);
        PARQUET_THROW_NOT_OK(builder.AppendValues(bvec, is_valid));
        std::shared_ptr<arrow::Array> ary;
        PARQUET_THROW_NOT_OK(builder.Finish(&ary));
        m_table_arrays.push_back(ary);
        m_table_fields.emplace_back(arrow::field(m_col_names[index], arrow::boolean()));
      }
        break;
      }
    }
    std::shared_ptr<arrow::Schema> schema = arrow::schema(m_table_fields);
    std::shared_ptr<arrow::Table>  t = arrow::Table::Make(schema, m_table_arrays);
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_THROW_NOT_OK(arrow::io::FileOutputStream::Open(m_filename, &outfile));
    PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(*t, arrow::default_memory_pool(), outfile, m_rows_per_group));
  }

private:
  DataFrame &m_df;
  std::string m_filename;
  std::vector<std::string> m_col_types;
  std::vector<int> m_col_idx;
  int m_rows_per_group;
  int m_verbose;
  std::vector<std::shared_ptr<arrow::Array>> m_table_arrays;
  std::vector<std::shared_ptr<arrow::Field>> m_table_fields;
  std::vector<std::string> m_col_names;
  std::set<int> m_col_idx_set;
  int m_cols;
  int m_rows;
  int m_row_groups;
  int m_row_remainder;
  std::shared_ptr<::arrow::io::FileOutputStream> m_out_file;
  static const std::unordered_map<std::string, arrow::Type::type> m_parquet_type_map;
}; // class Rparquet_Writer
const std::unordered_map<std::string, arrow::Type::type>
Rparquet_Writer::m_parquet_type_map = {{"integer", Type::type::INT32},
                                      {"integer64", Type::type::INT64},
                                      {"nanotime", Type::type::TIMESTAMP},
                                      {"numeric", Type::type::DOUBLE},
                                      {"character", Type::type::STRING},
                                      {"logical", Type::type::BOOL},
                                      {"factor", Type::type::STRING}};
} // namespace RParquet


// [[Rcpp::export]]
int write_rparquet(DataFrame &df, std::string filename, CharacterVector col_types, IntegerVector selected_col, int group_rows, int verbose) {
  try {
    RParquet::Rparquet_Writer rp_writer(df, filename, col_types, selected_col, group_rows, verbose);
    rp_writer.init();
    rp_writer.write_parquet();
  } catch (const std::exception & e) {
    stop(" Parquet what error: %s", e.what());
  }
  return 0;
}
