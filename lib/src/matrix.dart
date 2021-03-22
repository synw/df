import 'package:ml_linalg/vector.dart';

/// A class to manage the data inside the [DataFrame]
class DataMatrix {
  /// The dataset
  List<List<dynamic>> data = <List<dynamic>>[];

  // ********* insert operations **********

  /// Add a row
  void addRow(Map<String, dynamic> row, Map<int, String> indices) {
    //print("DF ADD ROW $row / $indices");
    final r = <dynamic>[];
    var i = 0;
    row.forEach((k, dynamic v) {
      final keyName = indices[i];
      r.add(row[keyName]);
      ++i;
    });
    data.add(r);
  }

  // ********* select operations **********

  /// Row for an index position
  Map<String, dynamic> rowForIndex(
      int index, Map<int, String> indicesToColumnNames) {
    final row = <String, dynamic>{};
    final dataRow = data[index];
    var i = 0;
    dataRow.forEach((dynamic item) {
      row[indicesToColumnNames[i]!] = item;
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
        dataRow[indices[i]!] = item;
        ++i;
      });
      dataRows.add(dataRow);
    }
    return dataRows;
  }

  /// Get typed data from a column
  List<T?> typedRecordsForColumnIndex<T>(int columnIndex, {int? limit}) {
    final dataFound = <T?>[];
    var i = 0;
    for (final row in data) {
      print(row.runtimeType);
      T? val;
      try {
        val = row[columnIndex] as T;
      } catch (e) {
        rethrow;
        //throw TypeConversionException(
        //    "Can not convert record $val to type $T $e");
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
  int countForValues(int columnIndex, List<dynamic> values) {
    var n = 0;
    data.forEach((row) {
      if (values.contains(row[columnIndex])) {
        ++n;
      }
    });
    return n;
  }

  // ********* aggregations **********

  /// Sum a column
  double sumCol<T>(int columnIndex) {
    final rawData = typedRecordsForColumnIndex<T>(columnIndex);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.sum();
  }

  /// Mean a column
  double meanCol(int columnIndex) {
    final rawData = typedRecordsForColumnIndex<double>(columnIndex);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.mean();
  }

  /// Get the max value of a column
  double maxCol(int columnIndex) {
    final rawData = typedRecordsForColumnIndex<double>(columnIndex);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.max();
  }

  /// Get the min value of a column
  double minCol(int columnIndex) {
    final rawData = typedRecordsForColumnIndex<double>(columnIndex);
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
