context("test read and write of empty data frame, column types are integer64,integer,character,numeric,logical")

test_that("test header",{
  fp <- "../test_data/temp.parquet"
  w_df <- data.frame(bit64::integer64(), integer(), character(), numeric(), logical(), stringsAsFactors = F)
  RParquet::rparquet_writer(w_df,fp)
  r_df <- RParquet::rparquet_reader(fp)
  expect_true(all.equal(w_df,r_df))
  if (file.exists(fp)) file.remove(fp)
})
