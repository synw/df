import "package:test/test.dart";
import 'package:df/df.dart';

DataFrame baseDf;

void main() {
  DataFrame df;

  test("from rows", () async {
    final date = DateTime.now();
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": "a", "col2": 1, "col3": 1.0, "col4": date},
      <String, dynamic>{"col1": "b", "col2": 2, "col3": 2.0, "col4": date},
    ];
    df = DataFrame.fromRows(rows);
    expect(df.length, 2);
    expect(df.columnsNames, <String>["col1", "col2", "col3", "col4"]);
    expect(df.rows, rows);
    expect(df.dataset, <dynamic>[
      <dynamic>["a", 1, 1.0, date],
      <dynamic>["b", 2, 2.0, date]
    ]);
    expect(df.colRecords<String>("col1"), <String>["a", "b"]);
    expect(df.colRecords<String>("col1", limit: 1), <String>["a"]);
    final cols = <DataFrameColumn>[
      DataFrameColumn(name: "col1", type: String),
      DataFrameColumn(name: "col2", type: int),
      DataFrameColumn(name: "col3", type: double),
      DataFrameColumn(name: "col4", type: DateTime),
    ];
    expect(df.columns, cols);
    // errors
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
    // date
    df = await DataFrame.fromCsv("test/data/data_date.csv",
        dateFormat: "MMM dd yyyy", verbose: true)
      ..show();
    expect(df.length, 2);
    expect(df.columnsNames, <String>["symbol", "date", "price", "n"]);

    // date iso
    df = await DataFrame.fromCsv("test/data/data_date_iso.csv", verbose: true)
      ..show();
    expect(df.length, 2);
    expect(df.columnsNames, <String>["symbol", "date", "price", "n"]);

    // timestamp
    df = await DataFrame.fromCsv("test/data/data_timestamp_ms.csv",
        timestampCol: "timestamp", verbose: true)
      ..show();
    expect(df.columnsNames, <String>["symbol", "price", "n", "timestamp"]);

    // timestamp microseconds
    df = await DataFrame.fromCsv("test/data/data_timestamp_mi.csv",
        timestampCol: "timestamp",
        timestampFormat: TimestampFormat.microseconds,
        verbose: true)
      ..show();
    expect(df.columnsNames, <String>["symbol", "price", "n", "timestamp"]);

    // timestamp seconds
    df = await DataFrame.fromCsv("test/data/data_timestamp_s.csv",
        timestampCol: "timestamp",
        timestampFormat: TimestampFormat.seconds,
        verbose: true)
      ..show();
    expect(df.columnsNames, <String>["symbol", "price", "n", "timestamp"]);

    final df2 = df.copy_();
    expect(df2.length, df.length);

    df = await DataFrame.fromCsv("/wrong/path").catchError((dynamic e) {
      expect(e.runtimeType.toString() == "FileNotFoundException", true);
      expect(e.message, 'File not found: /wrong/path');
    });

    df = await DataFrame.fromCsv("test/data/data_timestamp_s.csv",
        timestampCol: "timestamp",
        timestampFormat: TimestampFormat.seconds,
        verbose: true)
      ..show();
    expect(df.columnsNames, <String>["symbol", "price", "n", "timestamp"]);
  });

  test("subset", () async {
    baseDf = await DataFrame.fromCsv("example/dataset/stocks.csv");
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
    df = df2.limit_(100);
    expect(df.length, 30);
    df = df2.limit_(100, startIndex: 10);
    expect(df.length, 20);
    df.limit(30);
    expect(df.length, 20);
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
    df = DataFrame.fromRows(rows);
    final recs = <List<int>>[
      [2, 3]
    ];
    df.addRecords(recs);
    expect(df.length, 3);
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
      expect(e.toString(),
          "type 'int' is not a subtype of type 'double' in type cast");
    }
    try {
      DataFrameColumn.inferFromRecord("1", null);
    } catch (e) {
      expect(e is AssertionError, true);
    }
    try {
      DataFrameColumn.inferFromRecord(null, "n");
    } catch (e) {
      expect(e is AssertionError, true);
    }
  });

  test("sort", () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{"col1": 1, "col2": 4},
      <String, dynamic>{"col1": 2, "col2": 3},
      <String, dynamic>{"col1": 3, "col2": 2},
      <String, dynamic>{"col1": 4, "col2": 1},
    ];
    df = DataFrame.fromRows(rows)
      ..head()
      ..sort("col2");
    expect(df.colRecords<int>("col1"), <int>[4, 3, 2, 1]);
    final df2 = df.sort_("col1");
    expect(df2.colRecords<int>("col1"), <int>[1, 2, 3, 4]);
    try {
      df.sort_("wrong_col");
    } catch (e) {
      expect(e is ColumnNotFoundException, true);
    }
    try {
      df.sort_(null);
    } catch (e) {
      expect(e is AssertionError, true);
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
    expect(df.columnsIndices, <int, String>{0: "col1", 1: "col2"});
    expect(df.columnIndice("col1"), 0);
  });

  test("type inference", () async {
    var r = DataFrameColumn.inferFromRecord("0", "record");
    expect(r.type, int);
    r = DataFrameColumn.inferFromRecord("foo", "record");
    expect(r.type, String);
    r = DataFrameColumn.inferFromRecord(
        DateTime.now().toIso8601String(), "record");
    expect(r.type, DateTime);
  });

  test("set", () async {
    final edf = ExtendedDf();
    final columns = <DataFrameColumn>[
      DataFrameColumn(name: "col1", type: int),
      DataFrameColumn(name: "col2", type: double),
    ];
    edf.setColumns(columns);
    expect(edf.columns, columns);
    final dataset = [
      <dynamic>[1, 1.0],
      <dynamic>[2, 2.0]
    ];
    edf.dataset = dataset;
    expect(edf.dataset, dataset);
  });

  test("from stream", () async {
    final inputStream = Stream<String>.fromIterable([
      "a,b",
      "1,2"
    ]);
    df = await DataFrame.fromStream(inputStream);
    expect(df.columnsNames, ["a","b"]);
    expect(df.rows.toList(), [{"a": 1, "b": 2}]);
  });

  test("escape quotes", () async {
    final inputStream = Stream<String>.fromIterable([
      "a,\"b\"",
      "1,\"2\""
    ]);
    df = await DataFrame.fromStream(inputStream);
    // Escape quites should be consumed during parsing
    expect(df.columnsNames, ["a","b"]);
    expect(df.rows.toList(), [{"a": 1, "b": 2}]);
  });
}

class ExtendedDf extends DataFrame {}
