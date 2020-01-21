#!/bin/bash
# databricks notebook convert utility

INPUT_FILE=$1
OUTPUT_FILE=$2
MODE=$3

if [ ! "$MODE" == "--revert" ]; then
  # convert local to databricks
  #   remove stub, unindent main
  stub_flag=FALSE
  main_flag=FALSE
  cat $INPUT_FILE | while IFS= read line
  do
    # start stub
    if [[ "$line" == "# import spark-context and databricks stub / for unit testing" ]]; then
      stub_flag=TRUE
    # start main
    elif [[ "$line" == "if __name__ == '__main__':" ]]; then
      main_flag=TRUE
    # indent check
    elif [[ ! $line =~ (^[[:blank:]]+|^$) ]]; then
      main_flag=FALSE
      if [[ $line =~ (^#[[:blank:]]COMMAND) ]]; then
        stub_flag=FALSE
      fi
    fi
    # echo
    if [ "$stub_flag" == "FALSE" ]; then
      if [ "$main_flag" == "FALSE" ]; then
        echo "$line"
      else
        if [[ $line =~ ^[[:blank:]]+ ]]; then
          echo "${line:2}"
        fi
      fi
    fi
  done > $OUTPUT_FILE


else
  # convert databricks to local
  #   add stub, indent main
  echo "# import spark-context and databricks stub / for unit testing" > $OUTPUT_FILE
  echo "from stub import dbutils" >> $OUTPUT_FILE
  echo "from stub import display" >> $OUTPUT_FILE
  echo "import pyspark.sql" >> $OUTPUT_FILE
  echo "" >> $OUTPUT_FILE
  echo "spark = pyspark.sql.SparkSession.builder.appName('local').getOrCreate()" >> $OUTPUT_FILE
  echo "" >> $OUTPUT_FILE

  main_flag=FALSE
  cat $INPUT_FILE | while IFS= read line
  do
    # start/end main
    if [[ ! $line =~ (^[[:blank:]]+|^$|^def|^class|^from|^import|^#) ]]; then
      main_flag=TRUE
      echo "if __name__ == '__main__':"
    elif [[ ! $line =~ (^#[[:blank:]]COMMAND) ]]; then
      main_flag=FALSE
    fi
    if [ "$main_flag" == "TRUE" ]; then
      echo "  $line"
    else
      echo "$line"
    fi
  done >> $OUTPUT_FILE
fi
