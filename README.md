# RParquet #
## Overview ##
- RParquet is a tool for read/write R Data Frames from/into Apache Parquet file. 
- The tool operates on the data by using Apache Arrow API.
- It supports some data types mapping between R and Parquet.
- R types: "integer", "integer64", "nanotime", "numeric", "character", "logical"
- Arrow(Parquet): "INT32", "INT64", "TIMESTAMP", "DOUBLE", "STRING", "BOOL"
## Installation ##
- Please check [parquet-cpp](https://github.com/apache/parquet-cpp.git) to install the parquet-cpp library.
- The arrow version need to be changed when we cmake parquet-cpp

###### STEPS ######
    1. git clone https://github.com/apache/arrow.git
       git rev-parse --verify HEAD
       Get the current version hascode. ex. 0.10 version(94e8196ff925e2a8051ac330a02bee0dc63702c8)
       
    2. cd /parquet-cpp/cmake_modules
       vi ./ArrowExternalProject.cmake
       find the line contains set(ARROW_VERSION...)
       change the version hashcode to the one we got in step1. 
       ex. set(ARROW_VERSION "94e8196ff925e2a8051ac330a02bee0dc63702c8")
    
    3. In R(>=3.4), we need to install some packages
       install.packages(c("curl", "httr", "xml2", "devtools", "roxygen2", "testthat", "knitr", "nanotime", "bit64"))
    
    4. Set environment variables for compile dependences
       There are three parts need to be set:
         "parquet-cpp/src", "parquet-cpp/build/.../include" and boost library path("/usr/lib/x86_64-linux-gnu")
       For example, in RStudio or R terminal:
         Sys.setenv(CPLUS_INCLUDE_PATH="parquet-cpp/src:parquet-cpp/build/latest/include")
         Sys.setenv(LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:parquet-cpp/build/latest")
         Sys.setenv(LD_LIBRARY_PATH="/usr/lib/x86_64-linux-gnu:parquet-cpp/build/latest")
         Sys.setenv(LD_RUN_PATH="/usr/lib/x86_64-linux-gnu:parquet-cpp/build/latest")
    
    5. Build RParquet library
       In RStudio:
         Build -> Install and Restart
       In R terminal:
         library(devtools)
         install()
         library(RParquet)
         
## Features ##
     TODO
