import 'dart:math';

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
          {int? offset, int? limit}) =>
      data
          .sublist(offset ?? 0, limit)
          .map((row) => typedRecordForColumnIndexInRow<T>(columnIndex, row))
          .toList();

  /// Get typed data for a specific column in a row.
  T? typedRecordForColumnIndexInRow<T>(int columnIndex, List<Object?> row) {
    final rawVal = row[columnIndex];
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
  double sumCol(int columnIndex) {
    return _getVector(columnIndex, NullMeanBehavior.skip)
        .reduce((total, val) => total + val);
  }

  /// Mean a column
  double meanCol(int columnIndex, {required NullMeanBehavior nullBehavior}) {
    return sumCol(columnIndex) / _getVector(columnIndex, nullBehavior).length;
  }

  /// Get the max value of a column
  double maxCol(int columnIndex) {
    return _getVector(columnIndex, NullMeanBehavior.skip).reduce(max);
  }

  /// Get the min value of a column
  double minCol(int columnIndex) {
    return _getVector(columnIndex, NullMeanBehavior.skip).reduce(min);
  }

  // ***********************
  // Internal methods
  // ***********************

  List<double> _getVector(int columnIndex, NullMeanBehavior nullBehavior) {
    final rawData = typedRecordsForColumnIndex<num>(columnIndex);
    final nullFiltered = nullBehavior == NullMeanBehavior.skip
        ? rawData.where((e) => e != null)
        : rawData.map((e) => e ?? 0.0);
    // Cast is safe because nulls were eliminated above out above.
    return nullFiltered.map((e) => e!.toDouble()).toList();
  }
}
