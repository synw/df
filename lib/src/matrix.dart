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
    for (var i = 0; i < indices.length; i++) {
      final keyName = indices[i];
      r.add(row[keyName]);
    }
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
      T? val;
      try {
        val = row[columnIndex] as T?;
      } catch (e) {
        rethrow;
        //throw TypeConversionException(
        //    "Can not convert record $val to type $T $e");
      }
      dataFound.add(val);
      i++;
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
    return _getVector(columnIndex, NullAggregation.skip).sum();
  }

  /// Mean a column
  double meanCol(int columnIndex, {required NullAggregation nullAggregation}) {
    return _getVector(columnIndex, nullAggregation).mean();
  }

  /// Get the max value of a column
  double maxCol(int columnIndex) {
    return _getVector(columnIndex, NullAggregation.skip).max();
  }

  /// Get the min value of a column
  double minCol(int columnIndex) {
    return _getVector(columnIndex, NullAggregation.skip).min();
  }

  // ***********************
  // Internal methods
  // ***********************

  Vector _getVector(int columnIndex, NullAggregation nullAggregation) {
    final rawData = typedRecordsForColumnIndex<num>(columnIndex);
    final nullFiltered = nullAggregation == NullAggregation.skip
        ? rawData.where((e) => e != null)
        : rawData.map((e) => e ?? 0.0);
    // Cast is safe because nulls were eliminated above out above.
    return Vector.fromList(nullFiltered.map((e) => e!).toList());
  }
}

/// How to treat nulls when aggregating a column. Only applicable to the mean
/// aggregation - min, max and sum all use skip aggregation.
enum NullAggregation {
  /// Skip null values.
  ///   eg mean(1, 2, null) => (1 + 2) / 2.0 => 1.5
  skip,

  /// convert null values to zero.
  ///   eg mean(1, 2, null) => (1 + 1 + 0) / 3.0 => 1
  zero,
}
