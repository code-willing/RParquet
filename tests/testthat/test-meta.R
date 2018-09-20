context("consistent meta reader")
fp <- "../test_data/data.parquet"
test_that("consistent meta reader",{
  system.time(df <- rparquet_metadata(fp))
  expect_true(all((df$COL_PHY_TYPE == 'INT64')|(df$COL_PHY_TYPE == 'DOUBLE')|(df$COL_PHY_TYPE == 'BYTE_ARRAY')|(df$COL_PHY_TYPE == 'BOOLEAN')))
})

context("consistent meta reading across writer")
test_that("consistent meta reading across writer",{
  w_fp <- "../test_data/temp.parquet"
  r_df1 <- rparquet_reader(fp)
  rparquet_writer(r_df1, w_fp)
  r_md1 <- rparquet_metadata(w_fp)
  expect_true(file.remove(w_fp))
  r_df2 <- rparquet_reader(fp)
  rparquet_writer(r_df2, w_fp)
  r_md2 <- rparquet_metadata(w_fp)
  expect_true(file.remove(w_fp))
  expect_equal(r_md1, r_md2)
})
