#' RParquet: A package for read and write parquet file in R
#'
#' The RParquet package provides two basic functions:
#' rparquet_reader, rparquet_writer
#' @docType package
#' @import Rcpp
#' @importFrom data.table is.data.table
#' @useDynLib RParquet
#' @name RParquet
NULL

#' This function writes the dataframe based on the columns selected to the parquet file.
#' It supports "integer", "integer64", "nanotime", "numeric", "character", "logical" types.
#' @title Write the R DataFrame into a parquet file
#' @param df - A DataFrame to write
#' @param filename - A string. Specifies the parquet filename
#' @param columns - A integer vector. Specifies the wanted columns, first colum is 1.
#' Default value is to select all the columns.
#' @param group_rows - A integer. Specifies num of rows per group.
#' @param verbose - A integer.0-no verbose output, 1-regular verbose output, including
#' row/col/type etc
#' @return 0
#' @examples
#' \dontrun{
#' f <- "path_to_file.csv"
#' df <- read.csv(f)
#' rparquet_writer(df, f)
#'
#' rparquet_writer(df, f, columns = c(1,2), group_rows = 100)
#' }
#' @rdname rparquet_writer
#' @export
rparquet_writer <-
  function(df,
           filename,
           columns = c(-1),
           group_rows = 1000000,
           verbose = 0)
  {
    if (missing(df))
      stop("DataFrame is required.")

    if (missing(filename))
      stop("Filename is required")

    if (ncol(df) == 0)
      stop("Expected : ncol(df) > 0")

    if (nrow(df) == 0)
    {
      li <- list()
      for (i in 1:ncol(df))
      {
        li <- c(li, list(c(NA)))
      }
      df[nrow(df) + 1,] <- li
    }
    types <- as.character(lapply(df, class))
    write_rparquet(df, filename, types, columns, group_rows, verbose)
  }

#' This function returns a dataframe based on the columns selected in the parquet file.
#' It supports "INT32", "INT64", "TIMESTAMP", "DOUBLE", "STRING", "BOOL" Apache Arrow types.
#' @title Read the R DataFrame from a parquet file
#' @param filename - A string. Specifies the name of the parquet file
#' @param columns - An integer vector. Specifies the wanted columns. default is to select all the columns
#' @param filter - A logical vector. Specifies T/F for each row. default is all row selected
#' @param row_size - An integer. Specify num of rows to read per each read action.
#' @param threads - An integer. Specify read columns with the indicated level of parallelism.
#' @param verbose - An integer. 0-no verbose output, 1-regular verbose output, including
#' row/col/type etc
#' @return DataFrame
#' @examples
#' \dontrun{
#' f <- "path_to_file.parquet"
#'
#' df <- rparquet_reader(f)
#'
#' cols <- c(1,2,4,7,8,10)
#' df <- rparquet_reader(filename, columns)
#'
#' fdf <- read.csv("parth_to_file.csv");
#' filter = (fdf$sid > 50031571 & fdf$sid != 2018)
#' df <- rparquet_reader(filename, columns, filter)
#' }
#' @rdname rparquet_reader
#' @export
rparquet_reader <-
  function(filename,
           columns = c(-1),
           filter = c(TRUE) ,
           row_size = 100000,
           threads = 0,
           verbose = 0) {
    if (missing(filename))
      stop("Please provide filename")

    if (grepl(filename, ".parquet"))
      stop("Please provide filename With .parquet postfix")

    data <-
      read_parquet(filename, columns, filter, row_size, threads, verbose)
    data <- as.data.frame(data)
    ctypes <- sapply(data, class)
    idx = 1

    for (i in ctypes) {
      if (i == "integer64" || i == "nanotime") {
        data[, idx][as.character(data[, idx]) == '9218868437227407266'] <-
          bit64::NA_integer64_
      }
      idx = idx + 1
    }

    x = sapply(data, is.factor)
    if (any(x == TRUE)) {
      data[x] = lapply(data[x], as.character)
    }

    if (nrow(data) == 1) {
      trunc <- TRUE
      for (i in 1:ncol(data)) {
        if (!is.na(data[i])) {
          trunc <- FALSE
          break
        }
      }
      if (trunc)
        return(data[0,])
    }
    return(data)
  }


#' This function returns a metadata summary based on the input parquet file.
#' @title Generate the parquet metadata information
#' @param filename - A string. Specifies the name of the parquet file
#' @param details - detailed information about the parquet file
#' @return DataFrame
#' @examples
#' \dontrun{
#' f <- "path_to_file.parquet"
#' df <- rparquet_metadata(f)
#'
#' }
#' @rdname rparquet_metadata
#' @export
rparquet_metadata <-
  function(filename, details = F) {
    data <- read_metadata(filename, details)
    return(data)
  }
