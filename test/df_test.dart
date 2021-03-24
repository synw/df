import 'package:df/src/matrix.dart';
import 'package:test/test.dart';
import 'package:df/df.dart';

DataFrame baseDf = DataFrame();

void main() {
  var df = DataFrame();

  test('from rows', () async {
    final date = DateTime.now();
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 'a', 'col2': 1, 'col3': 1.0, 'col4': date},
      <String, dynamic>{'col1': 'b', 'col2': 2, 'col3': 2.0, 'col4': date},
      <String, dynamic>{'col1': 'c', 'col2': 3, 'col3': null},
    ];
    df = DataFrame.fromRows(rows);
    expect(df.length, 3);
    expect(df.columnsNames, <String>['col1', 'col2', 'col3', 'col4']);
    expect(df.rows.toList(), rows);
    expect(df.dataset, <dynamic>[
      <dynamic>['a', 1, 1.0, date],
      <dynamic>['b', 2, 2.0, date],
      <dynamic>['c', 3, null, null],
    ]);
    expect(df.colRecords<String>('col1'), <String>['a', 'b', 'c']);
    expect(df.colRecords<String>('col1', limit: 1), <String>['a']);
    final cols = <DataFrameColumn>[
      DataFrameColumn(name: 'col1', type: String),
      DataFrameColumn(name: 'col2', type: int),
      DataFrameColumn(name: 'col3', type: double),
      DataFrameColumn(name: 'col4', type: DateTime),
    ];
    expect(df.columns, cols);
    // errors
    try {
      df = DataFrame.fromRows(<Map<String, dynamic>>[]);
    } catch (e) {
      expect(e is AssertionError, true);
    }
  });

  test('csv', () async {
    // With an newline and the end of the file (optional by csv standard).
    df = await DataFrame.fromCsv('test/data/terminating_newline.csv',
        verbose: true)
      ..show();
    expect(df.length, 1);
    expect(df.columnsNames, <String>['a', 'b', 'c']);
    expect(df.rows.first, {'a': 1, 'b': 2, 'c': 3});

    // date
    df = await DataFrame.fromCsv('test/data/data_date.csv',
        dateFormat: 'MMM dd yyyy', verbose: true)
      ..show();
    expect(df.length, 2);
    expect(df.columnsNames, <String>['symbol', 'date', 'price', 'n']);

    // date iso
    df = await DataFrame.fromCsv('test/data/data_date_iso.csv', verbose: true)
      ..show();
    expect(df.length, 2);
    expect(df.columnsNames, <String>['symbol', 'date', 'price', 'n']);

    // timestamp
    df = await DataFrame.fromCsv('test/data/data_timestamp_ms.csv',
        timestampCol: 'timestamp', verbose: true)
      ..show();
    expect(df.columnsNames, <String>['symbol', 'price', 'n', 'timestamp']);

    // timestamp microseconds
    df = await DataFrame.fromCsv('test/data/data_timestamp_mi.csv',
        timestampCol: 'timestamp',
        timestampFormat: TimestampFormat.microseconds,
        verbose: true)
      ..show();
    expect(df.columnsNames, <String>['symbol', 'price', 'n', 'timestamp']);

    // timestamp seconds
    df = await DataFrame.fromCsv('test/data/data_timestamp_s.csv',
        timestampCol: 'timestamp',
        timestampFormat: TimestampFormat.seconds,
        verbose: true)
      ..show();
    expect(df.columnsNames, <String>['symbol', 'price', 'n', 'timestamp']);

    final df2 = df.copy_();
    expect(df2.length, df.length);

    try {
      df = await DataFrame.fromCsv('/wrong/path');
      fail('Expected a `FileNotFound` exception.');
    } catch (e) {
      expect(e, isA<FileNotFoundException>());
    }
  });

  test('subset', () async {
    baseDf = await DataFrame.fromCsv('example/dataset/stocks.csv');
    df = baseDf..subset(0, 30);
    expect(df.length, 30);
    final df2 = df.subset_(0, 30);
    expect(df2.length, 30);
  });

  test('limit', () async {
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

  test('count', () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 0, 'col2': 'b'},
      <String, dynamic>{'col1': 1, 'col2': null},
    ];
    df = DataFrame.fromRows(rows)..show();
    final z = df.countZeros_('col1');
    expect(z, 1);
    final n = df.countNulls_('col2');
    expect(n, 1);
  });

  test('mutate', () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 0, 'col2': 4},
      <String, dynamic>{'col1': 1, 'col2': 2},
    ];
    df = DataFrame.fromRows(rows)
      ..addRow(<String, dynamic>{'col1': 4, 'col2': 2});
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

  test('calc', () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 1, 'col2': 2},
      <String, dynamic>{'col1': 1, 'col2': 1},
      <String, dynamic>{'col1': null},
    ];
    df = DataFrame.fromRows(rows)..head();
    expect(df.max_('col2'), 2.0);
    expect(df.min_('col2'), 1.0);
    expect(df.mean_('col1', nullAggregation: NullAggregation.skip), 1.0);
    expect(df.mean_('col2', nullAggregation: NullAggregation.skip), 1.5);
    expect(df.mean_('col1', nullAggregation: NullAggregation.zero), 2.0 / 3.0);
    expect(df.mean_('col2', nullAggregation: NullAggregation.zero), 1.0);
    expect(df.sum_('col1'), 2.0);
  });

  test('error', () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 1, 'col2': 2},
      <String, dynamic>{'col1': 1, 'col2': 1},
    ];
    df = DataFrame.fromRows(rows)..cols();
    try {
      df.sum_('wrong_col');
    } catch (e) {
      expect(e is ColumnNotFoundException, true);
    }
    try {
      df.colRecords<double>('col1');
    } catch (e) {
      expect(e.toString(),
          'type \'int\' is not a subtype of type \'double\' in type cast');
    }
  });

  test('sort', () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 1, 'col2': 4},
      <String, dynamic>{'col1': 2, 'col2': 3},
      <String, dynamic>{'col1': 3, 'col2': 2},
      <String, dynamic>{'col1': 4, 'col2': 1},
    ];
    df = DataFrame.fromRows(rows)
      ..head()
      ..sort('col2');
    expect(df.colRecords<int>('col1'), <int>[4, 3, 2, 1]);
    final df2 = df.sort_('col1')!;
    expect(df2.colRecords<int>('col1'), <int>[1, 2, 3, 4]);
    try {
      df.sort_('wrong_col');
    } catch (e) {
      expect(e is ColumnNotFoundException, true);
    }
  });

  test('column', () async {
    final rows = <Map<String, dynamic>>[
      <String, dynamic>{'col1': 1, 'col2': 2},
      <String, dynamic>{'col1': 1, 'col2': 1},
    ];
    df = DataFrame.fromRows(rows)..head();
    final h = df.columns[0].hashCode;
    expect(h, 'col1'.hashCode);
    expect(df.columnsIndices, <int, String>{0: 'col1', 1: 'col2'});
    expect(df.columnIndex('col1'), 0);
  });

  test('type inference', () async {
    var r = DataFrameColumn.inferFromRecord('0', 'record');
    expect(r.type, int);
    r = DataFrameColumn.inferFromRecord('foo', 'record');
    expect(r.type, String);
    r = DataFrameColumn.inferFromRecord(
        DateTime.now().toIso8601String(), 'record');
    expect(r.type, DateTime);
    r = DataFrameColumn.inferFromRecord('\n0\n', 'record');
    expect(r.type, String);
  });

  test('set', () async {
    final edf = ExtendedDf();
    final columns = <DataFrameColumn>[
      DataFrameColumn(name: 'col1', type: int),
      DataFrameColumn(name: 'col2', type: double),
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

  test('from stream', () async {
    final inputStream = Stream<String>.fromIterable('a,b\n1,2\n'.split(''));
    df = await DataFrame.fromCharStream(inputStream);
    expect(df.columnsNames, ['a', 'b']);
    expect(df.rows.toList(), [
      {'a': 1, 'b': 2}
    ]);
  });

  test('from stream errors', () async {
    var inputStream = Stream<String>.fromIterable('a,b\n1,2'.split(''));
    expect(
        DataFrame.fromCharStream(inputStream), throwsA(isA<AssertionError>()));

    inputStream = Stream<String>.fromIterable(['a', ',', 'bb', '\n']);
    expect(
        DataFrame.fromCharStream(inputStream), throwsA(isA<AssertionError>()));
  });

  test('escape quotes are consumed', () async {
    // Escape quotes should be consumed during parsing
    final inputStream = Stream<String>.fromIterable('a,"b"\n1,"2"\n'.split(''));
    df = await DataFrame.fromCharStream(inputStream);
    expect(df.columnsNames, ['a', 'b']);
    expect(df.rows.toList(), [
      {'a': 1, 'b': 2}
    ]);
  });

  test('commas and double quotes are properly escaped', () async {
    // Escape quotes should be consumed during parsing
    var inputStream =
        Stream<String>.fromIterable('a,"b,c"\n1,"2,3"\n'.split(''));
    df = await DataFrame.fromCharStream(inputStream);
    expect(df.columnsNames, ['a', 'b,c']);
    expect(df.rows.toList(), [
      {'a': 1, 'b,c': '2,3'}
    ]);

    // within an escaped sequence, double quotes can be included by replacing
    // them with two double quotes - RFC4180-2.7
    inputStream = Stream<String>.fromIterable(
        'a,"b,c"\n"""They may say I\'m a dreamer, but I\'m not""","2,3"\n'
            .split(''));
    df = await DataFrame.fromCharStream(inputStream);
    expect(df.columnsNames, ['a', 'b,c']);
    expect(df.rows.toList(), [
      {'a': '"They may say I\'m a dreamer, but I\'m not"', 'b,c': '2,3'}
    ]);
  });

  test('newlines are properly escaped', () async {
    // Escape quotes should be consumed during parsing
    final inputStream =
        Stream<String>.fromIterable('a,"b,\nc"\n1,"\n23\n"\n'.split(''));
    df = await DataFrame.fromCharStream(inputStream);
    expect(df.columnsNames, ['a', 'b,\nc']);
    expect(df.rows.toList(), [
      {'a': 1, 'b,\nc': '\n23\n'}
    ]);
  });
}

class ExtendedDf extends DataFrame {}
