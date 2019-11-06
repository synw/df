import 'package:meta/meta.dart';

import 'column.dart';

class DataFrameInfo {
  void printRows(List<dynamic> rows) {
    for (final row in rows) {
      print(row.join(","));
    }
  }

  void colsInfo({@required List<DataFrameColumn> columns}) {
    for (final col in columns) {
      print("Column $col");
    }
  }
}
