import 'package:meta/meta.dart';

import 'column.dart';

class GeoDataFrameInfo {
  void head({
    @required int lines,
    @required List<String> columnsNames,
    @required List<List<dynamic>> data,
  }) {
    print(columnsNames.join(","));
    final rows = data.sublist(0, lines);
    printRows(rows);
  }

  void printRows(List<dynamic> rows) {
    for (final row in rows) {
      print(row.join(","));
    }
  }

  void colsInfo({@required List<DataFrameColumn> columns}) {
    final otherCols = <DataFrameColumn>[];
    for (final col in columns) {
      otherCols.add(col);
    }
    // print
    for (final col in otherCols) {
      print("Column $col");
    }
  }
}
