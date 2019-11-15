import "package:test/test.dart";
import 'package:df/df.dart';

DataFrame baseDf;

void main() {
  DataFrame df;

  test("from rows", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": "a", "col2": "b"},
      <String, dynamic>{"col1": "a", "col2": "b"},
    ];
    df = DataFrame.fromRows(rows);
    expect(df.length, 2);
    expect(df.columnsNames, <String>["col1", "col2"]);
    expect(df.rows, rows);
    expect(df.dataset, <dynamic>[
      <dynamic>["a", "b"],
      <dynamic>["a", "b"]
    ]);
    expect(df.colRecords<String>("col1"), <String>["a", "a"]);
    expect(df.colRecords<String>("col1", limit: 1), <String>["a"]);
    final cols = <DataFrameColumn>[
      DataFrameColumn(name: "col1", type: String),
      DataFrameColumn(name: "col2", type: String),
    ];
    expect(df.columns, cols);

    try {
      df = DataFrame.fromRows(null);
    } catch (e) {
      expect(e is AssertionError, true);
    }
    try {
      df = DataFrame.fromRows(<Map<String, dynamic>>[]);
    } catch (e) {
      expect(e is AssertionError, true);
    }
  });

  test("csv", () async {
    df = await DataFrame.fromCsv("example/dataset/stocks.csv")
      ..show();
    expect(df.length, 560);
    expect(df.columnsNames, <String>["symbol", "date", "price"]);
    baseDf = df.copy_();
    expect(df.length, baseDf.length);

    df = await DataFrame.fromCsv("/wrong/path").catchError((dynamic e) {
      expect(e.runtimeType.toString() == "FileNotFoundException", true);
      expect(e.message, 'File not found: /wrong/path');
    });
  });

  test("subset", () async {
    df = baseDf..subset(0, 30);
    expect(df.length, 30);
    final df2 = df.subset_(0, 30);
    expect(df2.length, 30);
  });

  test("limit", () async {
    df = baseDf..limit(30);
    expect(df.length, 30);
    final df2 = df.limit_(30);
    expect(df2.length, 30);
  });

  test("count", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": 0, "col2": "b"},
      <String, dynamic>{"col1": 1, "col2": null},
    ];
    df = DataFrame.fromRows(rows)..show();
    final z = df.countZeros_("col1");
    expect(z, 1);
    final n = df.countNulls_("col2");
    expect(n, 1);
  });

  test("mutate", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": 0, "col2": 4},
      <String, dynamic>{"col1": 1, "col2": 2},
    ];
    df = DataFrame.fromRows(rows)
      ..addRow(<String, dynamic>{"col1": 4, "col2": 2});
    expect(df.length, 3);
    df.removeRowAt(2);
    expect(df.rows, rows);
    df.removeFirstRow();
    expect(df.length, 1);
    df.removeLastRow();
    expect(df.length, 0);
  });

  test("calc", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": 1, "col2": 2},
      <String, dynamic>{"col1": 1, "col2": 1},
    ];
    df = DataFrame.fromRows(rows)..head();
    expect(df.max_("col2"), 2);
    expect(df.min_("col2"), 1);
    expect(df.mean_("col1"), 1);
    expect(df.sum_("col1"), 2);
  });

  test("error", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": 1, "col2": 2},
      <String, dynamic>{"col1": 1, "col2": 1},
    ];
    df = DataFrame.fromRows(rows)..cols();
    try {
      df.sum_("wrong_col");
    } catch (e) {
      expect(e is ColumnNotFoundException, true);
    }
    try {
      df.colRecords<double>("col1");
    } catch (e) {
      expect(e is TypeConversionException, true);
    }
  });

  test("column", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": 1, "col2": 2},
      <String, dynamic>{"col1": 1, "col2": 1},
    ];
    df = DataFrame.fromRows(rows)..head();
    final h = df.columns[0].hashCode;
    expect(h, "col1".hashCode);
  });
}
