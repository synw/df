import 'dart:async';
import 'dart:convert';
import 'dart:io';

import 'exceptions.dart';
import 'models/column.dart';
import 'models/info.dart';
import 'models/matrix.dart';

class DataFrame {
  DataFrame();

  List<DataFrameColumn> _columns = <DataFrameColumn>[];
  final DataMatrix _matrix = DataMatrix();
  final _info = GeoDataFrameInfo();
  Map<int, String> _columnsIndices;

  // ***********************
  // Getters
  // ***********************

  // ********* data **********

  Iterable<Map<String, dynamic>> get rows => _iterRows();

  List<List<dynamic>> get records => _matrix.data;

  // ********* info **********

  int get length => _matrix.data.length;

  List<DataFrameColumn> get columns => _columns;

  List<String> get columnsNames =>
      List<String>.from(_columns.map<String>((c) => c.name));

  // ***********************
  // Constructors
  // ***********************

  DataFrame.fromRows(List<Map<String, dynamic>> rows)
      : assert(rows != null),
        assert(rows.isNotEmpty) {
    // create _columns from the first datapint
    rows[0].forEach((k, dynamic v) {
      final t = v.runtimeType as Type;
      _columns.add(DataFrameColumn(name: k, type: t));
    });
    _setColumnsIndices();
    // fill the data
    rows.forEach((row) => _matrix.addRow(row, _columnsIndices));
  }

  static Future<DataFrame> fromCsv(String path) async {
    final file = File(path);
    if (!file.existsSync()) {
      throw FileNotFoundException("File not found: $path");
    }
    final df = DataFrame();
    var i = 1;
    await file
        .openRead()
        .transform<String>(utf8.decoder)
        .transform<String>(const LineSplitter())
        .forEach((line) {
      print('line $i: $line');
      final vals = line.split(",");
      if (i == 1) {
        vals.forEach((v) {
          final t = v.runtimeType;
          df._columns.add(DataFrameColumn(name: v, type: t));
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
    _setColumnsIndices();
    _matrix.data = matrix;
  }

  // ***********************
  // Methods
  // ***********************

  // ********* select operations **********

  List<Map<String, dynamic>> subset(int startIndex, int endIndex) =>
      _matrix.rowsForIndexRange(startIndex, endIndex, _columnsIndices);

  DataFrame subset_(int startIndex, int endIndex) {
    final _newMatrix = _matrix.data.sublist(startIndex, endIndex);
    return DataFrame._copyWithMatrix(this, _newMatrix);
  }

  List<T> colRecords<T>(String colName) =>
      _matrix.typedRecordsForColumnIndice<T>(_indiceForColumn(colName));

  // ********* filter operations **********

  void limit(int max, {int startIndex = 0}) =>
      _matrix.data = _matrix.data.sublist(startIndex, startIndex + max);

  DataFrame limit_(int max, {int startIndex = 0}) {
    final _newMatrix = _matrix.data.sublist(startIndex, startIndex + max);
    return DataFrame._copyWithMatrix(this, _newMatrix);
  }

  // ********* count operations **********

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

  int countZeros_(String colName,
      {List<dynamic> zeroValues = const <dynamic>[0]}) {
    final n = _matrix.countForValues(_indiceForColumn(colName), zeroValues);
    return n;
  }

  // ********* insert operations **********

  void addRow(Map<String, dynamic> row) => _matrix.addRow(row, _columnsIndices);

  // ********* delete operations **********

  void removeRowAt(int index) => _matrix.data.removeAt(index);

  void removeFirstRow() => _matrix.data.removeAt(0);

  void removeLastRow() => _matrix.data.removeLast();

  // ********* dataframe operations **********

  DataFrame copy_() => DataFrame._copyWithMatrix(this, _matrix.data);

  // ********* calculations **********

  double sum(String colName) => _matrix.sumCol<num>(_indiceForColumn(colName));

  double mean(String colName) =>
      _matrix.meanCol<num>(_indiceForColumn(colName));

  double max(String colName) => _matrix.maxCol<num>(_indiceForColumn(colName));

  double min(String colName) => _matrix.minCol<num>(_indiceForColumn(colName));

  // ********* info **********

  void head([int lines = 5]) {
    print("${_columns.length} columns: ${columnsNames.join(",")}");
    final rows = _matrix.data.sublist(0, lines);
    _info.printRows(rows);
    print("$length rows");
  }

  void show([int lines = 5]) {
    print("${_columns.length} columns and $length rows");
    head(lines);
  }

  // ***********************
  // Internal methods
  // ***********************

  Iterable<Map<String, dynamic>> _iterRows() sync* {
    var i = 0;
    while (i < _matrix.data.length) {
      yield _matrix.rowsForIndex(i, _columnsIndices);
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

  Map<int, String> _setColumnsIndices() {
    final ind = <int, String>{};
    var i = 0;
    for (final col in _columns) {
      ind[i] = col.name;
      ++i;
    }
    _columnsIndices = ind;
    return ind;
  }
}
