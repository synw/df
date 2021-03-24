import 'package:ml_linalg/vector.dart';

import '../df.dart';

/// A class to manage the data inside the [DataFrame]
class DataMatrix {
  /// The dataset
  List<List<Object?>> data = [];

  // ********* insert operations **********

  /// Add a row
  void addRow(Map<String, Object?> row, List<String> columnNames) =>
      data.add(columnNames.map((colName) => row[colName]).toList());

  // ********* select operations **********

  /// Row for an index position
  Map<String, Object> rowForIndex(
      int index, List<String> indicesToColumnNames) {
    final row = <String, Object>{};
    final dataRow = data[index];
    var i = 0;
    dataRow.forEach((item) {
      if (item != null) row[indicesToColumnNames[i]] = item;
      i++;
    });
    return row;
  }

  /// Rows for an index range of positions
  List<Map<String, Object>> rowsForIndexRange(
      int startIndex, int endIndex, List<String> columnNames) {
    final dataRows = <Map<String, Object>>[];
    for (final row in data.sublist(startIndex, endIndex)) {
      final dataRow = <String, Object>{};
      var i = 0;
      row.forEach((Object? item) {
        if (item != null) dataRow[columnNames[i]] = item;
        i++;
      });
      dataRows.add(dataRow);
    }
    return dataRows;
  }

  /// Get typed data from a column
  List<T?> typedRecordsForColumnIndex<T>(int columnIndex,
      {int? offset, int? limit}) {
    final dataFound = <T?>[];
    var i = offset ?? 0;
    for (final row in data) {
      dataFound.add(typedRecordForColumnIndexInRow(columnIndex, row));
      i++;
      if (limit != null && i >= limit) {
        break;
      }
    }
    return dataFound;
  }

  /// Get typed data for a specific column in a row.
  T? typedRecordForColumnIndexInRow<T>(int columnIndex, List<Object?> row) {
    final rawVal = row[columnIndex];
    T? typedVal;
    if (!(rawVal is T?)) {
      throw ArgumentError(
          'Requested the record ($rawVal) as a $T at index $columnIndex of the following row:\n\t$row\n '
          'but the record is a ${rawVal.runtimeType} which is not a subtype of $T.');
    }
    return row[columnIndex] as T?;
  }

  // ********* count operations **********

  /// Count values in a column
  int countForValues(int columnIndex, List<Object?> values) {
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
    return _getVector(columnIndex, NullMeanBehavior.skip).sum();
  }

  /// Mean a column
  double meanCol(int columnIndex, {required NullMeanBehavior nullAggregation}) {
    return _getVector(columnIndex, nullAggregation).mean();
  }

  /// Get the max value of a column
  double maxCol(int columnIndex) {
    return _getVector(columnIndex, NullMeanBehavior.skip).max();
  }

  /// Get the min value of a column
  double minCol(int columnIndex) {
    return _getVector(columnIndex, NullMeanBehavior.skip).min();
  }

  // ***********************
  // Internal methods
  // ***********************

  Vector _getVector(int columnIndex, NullMeanBehavior nullBehavior) {
    final rawData = typedRecordsForColumnIndex<num>(columnIndex);
    final nullFiltered = nullBehavior == NullMeanBehavior.skip
        ? rawData.where((e) => e != null)
        : rawData.map((e) => e ?? 0.0);
    // Cast is safe because nulls were eliminated above out above.
    return Vector.fromList(nullFiltered.map((e) => e!).toList());
  }
}
