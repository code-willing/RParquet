#"nanotime"
#"integer64"
#"integer"
#"numeric"
#"character"
#"logical"
p_fp <- '../test_data/data.parquet'
c_fp <- '../test_data/data.csv.gz'
w_fp <- '../test_data/temp.parquet'

get_logicol<- function(df){
  for ( i in 1:ncol(df))
  {
    if (sapply(df[i], class) == 'logical')
    {
      return(df[i])
    }
  }
  return(NULL)
}

test_that("consistent logical column read",{
      df_csv <- read.csv(c_fp)
      rparquet_writer(df_csv, w_fp)
      df_parquet <- rparquet_reader(w_fp)
      
      x_csv <- get_logicol(df_csv)
      x_parquet <- get_logicol(df_parquet)
      expect_false(is.null(x_csv))
      expect_false(is.null(x_parquet))
      expect_equal(x_csv,x_parquet)
      expect_true(file.remove(w_fp))
    })

test_that("consistent int, double, string",{
      options(stringsAsFactors = FALSE)
      df_csv <- read.csv(c_fp)
      rparquet_writer(df_csv, w_fp)
      df_parquet <- rparquet_reader(w_fp)
      sapply(df_csv, class)
      sapply(df_parquet, class)
      expect_equal(df_csv,df_parquet)
      expect_true(file.remove(w_fp))
    })

context("test factor column won't affect reader")
test_that("stringAsFactor TRUE won't affect reader",{
      options(stringsAsFactors = TRUE)
      p_df <- rparquet_reader(p_fp)
      expect_true(all(sapply(p_df,class) != 'factor'))
      options(stringsAsFactors = FALSE)
    })
