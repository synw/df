import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'column.dart';
import 'exceptions.dart';
import 'info.dart';
import 'matrix.dart';

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

  /// Build a dataframe from a csv file
  static Future<DataFrame> fromCsv(String path) async {
    final file = File(path);
    if (!file.existsSync()) {
      throw FileNotFoundException("File not found: $path");
    }
    final df = DataFrame();
    var i = 1;
    List<String> colNames;
    await file
        .openRead()
        .transform<String>(utf8.decoder)
        .transform<String>(const LineSplitter())
        .forEach((line) {
      print('line $i: $line');
      final vals = line.split(",");
      if (i == 1) {
        // set columns names
        colNames = vals;
      } else if (i == 2) {
        // infer columns types from records
        var vi = 0;
        vals.forEach((v) {
          final col = DataFrameColumn.inferFromRecord(v, colNames[vi]);
          df._columns.add(col);
          ++vi;
        });
      } else {
        df._matrix.data.add(vals);
      }
      ++i;
    });
    print("Parsed ${df._matrix.data.length} rows");
    return df;
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
  void limit(int max, {int startIndex = 0}) =>
      _matrix.data = _matrix.data.sublist(startIndex, startIndex + max);

  /// Get a new dataframe with limited data
  DataFrame limit_(int max, {int startIndex = 0}) {
    final _newMatrix = _matrix.data.sublist(startIndex, startIndex + max);
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

  /*   DataFrame _sort(String colName,
      {bool inPlace = false, bool reverse = false}) {
    assert(colName != null);
    _isSortedBy = colName;
    final colIndice = _indiceForColumn(colName);
    final order = _sortIndexForIndice(colIndice);
    var _newMatrix = <List<dynamic>>[];
    for (final indice in order) {
      _newMatrix.add(_matrix.data[indice]);
    }
    if (reverse) {
      _newMatrix = _newMatrix.reversed.toList();
    }
    if (!inPlace) {
      return DataFrame._copyWithMatrix(this, _newMatrix);
    } else {
      _matrix.data = _newMatrix;
    }
    return null;
  }

  List<int> _sortIndexForIndice(int indice) {
    final order = <int>[];
    final values = _columnDataWithIndex(indice);
    final orderedValues = values.keys.toList()..sort();
    for (final value in orderedValues) {
      order.add(values[value]);
    }
    return order;
  }*/

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
