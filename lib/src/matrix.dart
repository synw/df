import 'package:ml_linalg/vector.dart';

import 'exceptions.dart';

class DataMatrix {
  List<List<dynamic>> data = <List<dynamic>>[];

  // ********* insert operations **********

  void addRow(Map<String, dynamic> record, Map<int, String> indices) {
    final row = <dynamic>[];
    var i = 0;
    record.forEach((k, dynamic v) {
      final keyName = indices[i];
      row.add(record[keyName]);
      ++i;
    });
    data.add(row);
  }

  // ********* select operations **********

  Map<String, dynamic> rowsForIndex(int index, Map<int, String> indices) {
    final row = <String, dynamic>{};
    final dataRow = data[index];
    var i = 0;
    dataRow.forEach((dynamic item) {
      row[indices[i]] = item;
      ++i;
    });
    return row;
  }

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

  double sumCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.sum();
  }

  double meanCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.mean();
  }

  double maxCol<T>(int columnIndice) {
    final rawData = typedRecordsForColumnIndice<T>(columnIndice);
    final data = List<double>.from(rawData.map<double>(_numToDouble));
    final vector = Vector.fromList(data);
    return vector.max();
  }

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
