import 'package:ml_linalg/vector.dart';

import 'column.dart';
import 'exceptions.dart';

/// A class to manage the data inside the [DataFrame]
class DataMatrix {
  /// The dataset
  List<List<dynamic>> data = <List<dynamic>>[];

  // ********* insert operations **********

  /// Add a row
  void addRow(Map<String, dynamic> row, Map<int, String> indices,
      List<DataFrameColumn> columns) {
    //print("DF ADD ROW $row / $indices");
    final r = <dynamic>[];
    columns.forEach((col) {
      dynamic v = row[col.name];
      // cast records to the right type
      switch (col.type) {
        case int:
          v = int.tryParse(v.toString());
          break;
        case double:
          v = double.tryParse(v.toString());
          break;
        case DateTime:
          v = DateTime.tryParse(v.toString());
          break;
        default:
      }
      r.add(v);
    });
    data.add(r);
  }

  // ********* select operations **********

  /// Row for an index position
  Map<String, dynamic> rowForIndex(int index, Map<int, String> indices) {
    final row = <String, dynamic>{};
    final dataRow = data[index];
    var i = 0;
    dataRow.forEach((dynamic item) {
      row[indices[i]] = item;
      ++i;
    });
    return row;
  }

  /// Rows for an index range of positions
  List<Map<String, dynamic>> rowsForIndexRange(
      int startIndex, int endIndex, Map<int, String> indices) {
    final dataRows = <Map<String, dynamic>>[];
    for (final row in data.sublist(startIndex, endIndex)) {
      final dataRow = <String, dynamic>{};
      var i = 0;
      row.forEach((dynamic item) {
        dataRow[indices[i]] = item;
        ++i;
      });
      dataRows.add(dataRow);
    }
    return dataRows;
  }

  /// Get typed data from a column
  List<T> typedRecordsForColumnIndice<T>(int columnIndice, {int limit}) {
    final dataFound = <T>[];
    var i = 0;
    for (final row in data) {
      T val;
      try {
        val = row[columnIndice] as T;
      } catch (e) {
        throw TypeConversionException(
            "Can not convert record $val to type $T $e");
      }
      dataFound.add(val);
      ++i;
      if (limit != null) {
        if (i >= limit) {
          break;
        }
      }
    }
    return dataFound;
  }

  // ********* count operations **********

  /// Count values in a column
  int countForValues(int columnIndice, List<dynamic> values) {
    var n = 0;
    data.forEach((row) {
      if (values.contains(row[columnIndice])) {
        ++n;
      }
    });
    return n;
  }

  // ********* aggregations **********

  /// Sum a column
  double sumCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.sum();
  }

  /// Mean a column
  double meanCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.mean();
  }

  /// Get the max value of a column
  double maxCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.max();
  }

  /// Get the min value of a column
  double minCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.min();
  }

  // ***********************
  // Internal methods
  // ***********************

  double _numToDouble<T>(T value) {
    double n;
    if (T == int || T == num) {
      n = (value as num).toDouble();
    } else {
      n = value as double;
    }
    return n;
  }
}
