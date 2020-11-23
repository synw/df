# Df

[![pub package](https://img.shields.io/pub/v/df.svg)](https://pub.dartlang.org/packages/df) [![Build Status](https://travis-ci.org/synw/df.svg?branch=master)](https://travis-ci.org/synw/df) [![Coverage Status](https://coveralls.io/repos/github/synw/df/badge.svg?branch=master)](https://coveralls.io/github/synw/df?branch=master)

A dataframe for Dart

## Usage

### Create

From csv:

   ```dart
   final df = await DataFrame.fromCsv("dataset/stocks.csv");
   ```
fromCSV parses files according to the csv standard, including support for escape double quotes (see: [RFC4180](https://tools.ietf.org/html/rfc4180)).

Note: the type of the records are infered from the data. The first line of the csv must contains the headers for the column names. Optional parameters:

**`dateFormat`**: the string format of the date: [reference](https://pub.dev/documentation/intl/latest/intl/DateFormat-class.html). Ex: `MMM dd yyyy`

**`timestampCol`**: the column to be treated as a timestamp. Ex: `timestamp`

**`timestampFormat`**: the format of the timestamp: seconds, milliseconds or microseconds. Ex: `TimestampFormat.microseconds`

**`verbose`**: set to true to print some info

From records:

   ```dart
   final rows = <Map<String, dynamic>> rows[
      <String, dynamic>{"col1": 21, "col2": "foo", "col3": DateTime.now()},
      <String, dynamic>{"col1": 22, "col2": "bar", "col3": DateTime.now()},
   ];
   final df = DataFrame.fromRows(rows);
   ```

### Select

   ```dart
   final List<Map<String, dynamic>> rows = df.rows;
   // select a subset of rows
   final List<Map<String, dynamic>> rows = df.subset(0,100);
   /// select records for a column
   final List<double> values = df.colRecords<double>("col2");
   /// select list of records
   final List<List<dynamic>> records = df.records;
   ```

### Mutate

Add data:

   ```dart
   // add a row
   df.addRow(<String,dynamic>{"col1": 1, "col2": 2.0});
   // add a line of records
   df.addRecord(<dynamic>[1, 2.0]);
   ```

Remove data:

   ```dart
   df.removeFirstRow();
   df.removeLastRow();
   // remove the third row
   df.removeRowAt(2);
   // limit the dataframe to 100 rows starting from index 30
   df.limit(100, startIndex: 30);
   ```

Copy a dataframe:

   ```dart
   // get a new dataframe from the existing one
   final DataFrame df2 = df.copy_();
   // get a new dataframe with limited data
   final DataFrame df2 = df.limit_(100);
   ```

### Count

Nulls and zeros:

   ```dart
   final int n = df.countNulls_("col1");
   final int n = df.countZeros_("col1");
   ```

Columns:

   ```dart
   final int mean = df.mean("col1");
   final int sum = df.sum("col1");
   final int max = df.max("col1");
   final int min = df.min("col1");
   ```

### Info

   ```dart
   final int numRecords= df.length;
   final List<DataFrameColumn> cols = df.columns;
   final List<String> colNames = df.columnsNames;
   // print info and sample data
   df.head();
   // like head with a bit more details
   df.show();
   ```

## Conventions

All the dataframe operations are inplace. All the methods that return
objects end with an underscore. Example:

   ```dart
   // inplace
   df.limit(30);
   // get a new dataframe with limited data
   final DataFrame df2 = df.limit_(30);
   ```

Vocabulary conventions:

- A **row** is a map of key/values pair
- A **record** is a single cell value
- An **index** is a row position
- An **indice** is a column position
