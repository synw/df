import 'column.dart';

/// A class to manage dataframe info prints
class DataFrameInfo {
  /// Print rows info
  void printRows(List<dynamic> rows) {
    for (final row in rows) {
      print(row.join(','));
    }
  }

  /// Print columns info
  void colsInfo({required List<DataFrameColumn> columns}) {
    for (final col in columns) {
      print('Column $col');
    }
  }
}
