import 'dart:async';
import 'dart:collection';
import 'dart:convert';
import 'dart:io';

import 'package:jiffy/jiffy.dart';
import 'package:meta/meta.dart';

import 'column.dart';
import 'exceptions.dart';
import 'info.dart';
import 'matrix.dart';
import 'type.dart';

/// The main dataframe class
class DataFrame {
  /// Default constructor
  DataFrame();

  List<DataFrameColumn> _columns = <DataFrameColumn>[];
  final DataMatrix _matrix = DataMatrix();
  final _info = DataFrameInfo();
  //Map<int, String> _columnsIndices;

  // ***********************
  // Getters
  // ***********************

  // ********* data **********

  /// An iterable of rows data
  Iterable<Map<String, dynamic>> get rows => _iterRows();

  /// All the data
  List<List<dynamic>> get dataset => _matrix.data;
  set dataset(List<List<dynamic>> dataPoints) => _matrix.data = dataPoints;

  // ********* info **********

  /// Number of rows og the data
  int get length => _matrix.data.length;

  /// The dataframe columns
  List<DataFrameColumn> get columns => _columns;

  /// The dataframe columns names
  List<String> get columnsNames =>
      List<String>.from(_columns.map<String>((c) => c.name));

  /// The dataframe columns indices
  Map<int, String> get columnsIndices => _columnsIndices();

  // ***********************
  // Constructors
  // ***********************

  /// Build a dataframe from a list of rows
  DataFrame.fromRows(List<Map<String, dynamic>> rows)
      : assert(rows != null),
        assert(rows.isNotEmpty) {
    // create _columns from the first datapint
    rows[0].forEach((k, dynamic v) {
      final t = v.runtimeType as Type;
      _columns.add(DataFrameColumn(name: k, type: t));
    });
    // fill the data
    rows.forEach((row) => _matrix.addRow(row, _columnsIndices()));
  }

  static List<dynamic> _parseLine(
      String line) {
    return line.split(",");
  }

  static List<dynamic> _parseVals(
      List<dynamic> vals, List<DataFrameColumn> columnsNames,
      {String dateFormat,
      String timestampCol,
      TimestampFormat timestampFormat}) {
    var vi = 0;
    final colValues = <dynamic>[];
    vals.forEach((dynamic v) {
      // cast records to the right type
      switch (columnsNames[vi].type) {
        case int:
          colValues.add(int.tryParse(v.toString()));
          break;
        case double:
          colValues.add(double.tryParse(v.toString()));
          break;
        case DateTime:
          if (dateFormat != null) {
            colValues.add(Jiffy(v.toString(), dateFormat).dateTime);
          } else {
            if (timestampCol == columnsNames[vi].name) {
              DateTime d;
              if (timestampFormat == TimestampFormat.seconds) {
                d = DateTime.fromMillisecondsSinceEpoch(
                    int.parse(v.toString()) * 1000);
              } else if (timestampFormat == TimestampFormat.milliseconds) {
                d = DateTime.fromMillisecondsSinceEpoch(
                    int.parse(v.toString()));
              } else if (timestampFormat == TimestampFormat.microseconds) {
                d = DateTime.fromMicrosecondsSinceEpoch(
                    int.parse(v.toString()));
              }
              colValues.add(d);
            } else {
              colValues.add(DateTime.tryParse(v.toString()));
            }
          }
          break;
        default:
          colValues.add(v);
      }
      ++vi;
    });
    return colValues;
  }

  /// Build a dataframe from a utf8 encoded stream of comma separated strings
  static Future<DataFrame> fromStream(Stream<String> stream,
      {String dateFormat,
        String timestampCol,
        TimestampFormat timestampFormat = TimestampFormat.milliseconds,
        bool verbose = false}) async {
    final df = DataFrame();
    var i = 1;
    List<String> _colNames;
    await stream.forEach((line) {
      //print('line $i: $line');
      final vals = _parseLine(line);
      if (i == 1) {
        // set columns names
        _colNames = vals;
      } else {
        var vi = 0;
        if (i == 2) {
          vals.forEach((v) {
            DataFrameColumn col;
            if (_colNames[vi] == timestampCol) {
              col = DataFrameColumn(name: _colNames[vi], type: DateTime);
            } else {
              col = DataFrameColumn.inferFromRecord(v, _colNames[vi],
                  dateFormat: dateFormat);
            }
            df._columns.add(col);
            ++vi;
          });
        }
        final colValues = _parseVals(vals, df._columns,
            dateFormat: dateFormat,
            timestampCol: timestampCol,
            timestampFormat: timestampFormat);
        df._matrix.data.add(colValues);
      }
      ++i;
    });
    if (verbose) {
      print("Parsed ${df._matrix.data.length} rows");
    }
    return df;
  }

  /// Build a dataframe from a csv file
  static Future<DataFrame> fromCsv(String path,
      {String dateFormat,
      String timestampCol,
      TimestampFormat timestampFormat = TimestampFormat.milliseconds,
      bool verbose = false}) async {
    final file = File(path);
    if (!file.existsSync()) {
      throw FileNotFoundException("File not found: $path");
    }

    return fromStream(
      file
        .openRead()
        .transform<String>(utf8.decoder)
        .transform<String>(const LineSplitter()),
      dateFormat: dateFormat,
      timestampCol: timestampCol,
      timestampFormat: timestampFormat,
      verbose: verbose,
    );
  }

  DataFrame._copyWithMatrix(DataFrame df, List<List<dynamic>> matrix) {
    _columns = df._columns;
    _matrix.data = matrix;
  }

  // ***********************
  // Methods
  // ***********************

  // ********* select operations **********

  /// Limit the dataframe to a subset of data
  List<Map<String, dynamic>> subset(int startIndex, int endIndex) {
    final data =
        _matrix.rowsForIndexRange(startIndex, endIndex, _columnsIndices());
    _matrix.data = _matrix.data.sublist(startIndex, endIndex);
    return data;
  }

  /// Get a new dataframe with a subset of data
  DataFrame subset_(int startIndex, int endIndex) {
    final _newMatrix = _matrix.data.sublist(startIndex, endIndex);
    return DataFrame._copyWithMatrix(this, _newMatrix);
  }

  /// Get typed records for a column
  List<T> colRecords<T>(String colName, {int limit}) => _matrix
      .typedRecordsForColumnIndice<T>(_indiceForColumn(colName), limit: limit);

  // ********* filter operations **********

  /// Limit the data
  void limit(int max, {int startIndex = 0}) {
    var n = startIndex + max;
    final dflen = _matrix.data.length;
    if (n > dflen) {
      if (startIndex == 0) {
        return;
      }
      n = dflen;
    }
    _matrix.data = _matrix.data.sublist(startIndex, n);
  }

  /// Get a new dataframe with limited data
  DataFrame limit_(int max, {int startIndex = 0}) {
    var n = startIndex + max;
    final dflen = _matrix.data.length;
    if (n > _matrix.data.length) {
      if (startIndex == 0) {
        return this;
      }
      n = dflen;
    }
    final _newMatrix = _matrix.data.sublist(startIndex, n);
    return DataFrame._copyWithMatrix(this, _newMatrix);
  }

  // ********* count operations **********

  /// Count null values
  ///
  /// It is possible to provide a custom list of values
  /// considered as null with [nullValues]
  int countNulls_(String colName,
      {List<dynamic> nullValues = const <dynamic>[
        null,
        "null",
        "nan",
        "NULL",
        "N/A"
      ]}) {
    final n = _matrix.countForValues(_indiceForColumn(colName), nullValues);
    return n;
  }

  /// Count zero values
  ///
  /// It is possible to provide a custom list of values
  /// considered as zero with [zeroValues]
  int countZeros_(String colName,
      {List<dynamic> zeroValues = const <dynamic>[0]}) {
    final n = _matrix.countForValues(_indiceForColumn(colName), zeroValues);
    return n;
  }

  // ********* insert operations **********

  /// Add a row to the data
  void addRow(Map<String, dynamic> row) =>
      _matrix.addRow(row, _columnsIndices());

  /// Add a line of records to the data
  void addRecords(List<dynamic> records) => _matrix.data.add(records);

  // ********* delete operations **********

  /// Remove a row at a given index position
  void removeRowAt(int index) => _matrix.data.removeAt(index);

  /// Remove the first row
  void removeFirstRow() => _matrix.data.removeAt(0);

  /// Remove the last row
  void removeLastRow() => _matrix.data.removeLast();

  // ********* dataframe operations **********

  /// Get a copy of a dataframe
  DataFrame copy_() => DataFrame._copyWithMatrix(this, _matrix.data);

  /// Set the dataframe columns
  ///
  /// Use this in constructors if you extend the [Df] class
  /// to set initial columns
  void setColumns(List<DataFrameColumn> cols) => _columns.addAll(cols);

  // ********* calculations **********

  /// Sum of a column
  double sum_(String colName) => _matrix.sumCol<num>(_indiceForColumn(colName));

  /// Mean of a column
  double mean_(String colName) =>
      _matrix.meanCol<num>(_indiceForColumn(colName));

  /// Get the max value of a column
  double max_(String colName) => _matrix.maxCol<num>(_indiceForColumn(colName));

  /// Get the min value of a column
  double min_(String colName) => _matrix.minCol<num>(_indiceForColumn(colName));

  // ********* info **********

  /// Print sample data
  void head([int lines = 5]) {
    var l = lines;
    if (length < lines) {
      l = length;
    }
    final rows = _matrix.data.sublist(0, l);
    _info.printRows(rows);
    print("$length rows");
  }

  /// Print info and sample data
  void show([int lines = 5]) {
    print(
        "${_columns.length} columns and $length rows: ${columnsNames.join(", ")}");
    var l = lines;
    if (length < lines) {
      l = length;
    }
    final rows = _matrix.data.sublist(0, l);
    _info.printRows(rows);
  }

  /// Print columns info
  void cols() => _info.colsInfo(columns: _columns);

  /// Get the indice of a column
  int columnIndice(String colName) => _indiceForColumn(colName);

  // ***********************
  // Internal methods
  // ***********************

  Iterable<Map<String, dynamic>> _iterRows() sync* {
    var i = 0;
    while (i < _matrix.data.length) {
      yield _matrix.rowForIndex(i, _columnsIndices());
      ++i;
    }
  }

  /// Get a new dataframe ssorted by a column
  DataFrame sort_(String colName) =>
      _sort(colName, inPlace: false) as DataFrame;

  /// Sort this dataframe by a column
  void sort(String colName) => _sort(colName, inPlace: true);

  dynamic _sort(String colName, {@required bool inPlace}) {
    assert(colName != null);
    final colData =
        _matrix.typedRecordsForColumnIndice<dynamic>(_indiceForColumn(colName));
    // create a map of index/data
    final dataIndex = <int, dynamic>{};
    var i = 0;
    colData.forEach((dynamic record) {
      dataIndex[i] = record;
      ++i;
    });
    // sort the index map from values
    final sortedKeys = dataIndex.keys.toList(growable: false)
      ..sort((k1, k2) => (dataIndex[k1] as dynamic)
          .compareTo(dataIndex[k2] as dynamic) as int);
    final sortedMap = LinkedHashMap<int, dynamic>.fromIterable(sortedKeys,
        key: (dynamic k) => k as int, value: (dynamic k) => dataIndex[k]);
    final order = sortedMap.keys;
    // rebuild the dataset in order
    final _newMatrix = <List<dynamic>>[];
    for (final i in order) {
      _newMatrix.add(_matrix.data[i]);
    }
    if (!inPlace) {
      return DataFrame._copyWithMatrix(this, _newMatrix);
    } else {
      _matrix.data = _newMatrix;
    }
    return null;
  }

  int _indiceForColumn(String colName) {
    int ind;
    var i = 0;
    for (final col in _columns) {
      if (colName == col.name) {
        ind = i;
        break;
      }
      ++i;
    }
    if (ind == null) {
      throw ColumnNotFoundException("Can not find column $colName");
    }
    return ind;
  }

  Map<int, String> _columnsIndices() {
    final ind = <int, String>{};
    var i = 0;
    for (final col in _columns) {
      ind[i] = col.name;
      ++i;
    }
    return ind;
  }
}
