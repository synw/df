import 'dart:async';

import 'package:df/src/util/csv_parser.dart';
import 'package:test/test.dart';

StreamIterator<String> _iterFromLine(String line) =>
    StreamIterator(Stream.fromIterable(line.split('')));

Future<String> _remaining(StreamIterator<String> charIter) async {
  final buff = StringBuffer(charIter.current);
  while (await charIter.moveNext()) {
    buff.write(charIter.current);
  }
  return buff.toString();
}

void main() {
  test('test parseField', () async {
    StreamIterator<String> iter;
    StringBuffer record;

    // Parse first field.
    iter = _iterFromLine('a,bc,def\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseField(record);
    expect(await _remaining(iter), ',bc,def\n');
    expect(record.toString(), 'a');

    // Parse second field with two chars.
    iter = _iterFromLine('bc,def\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseField(record);
    expect(await _remaining(iter), ',def\n');
    expect(record.toString(), 'bc');

    // Parse final field.
    iter = _iterFromLine('def\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseField(record);
    expect(await _remaining(iter), '\n');
    expect(record.toString(), 'def');

    // Parse empty field.
    iter = _iterFromLine(',,\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseField(record);
    expect(await _remaining(iter), ',,\n');
    expect(record.toString(), '');

    // Parse empty final field.
    iter = _iterFromLine('\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseField(record);
    expect(await _remaining(iter), '\n');
    expect(record.toString(), '');
  });

  test('test parseField errors', () async {
    StreamIterator<String> iter;
    StringBuffer record;

    // A double quote in an unescaped field throws an error.
    iter = _iterFromLine('b"\n');
    await iter.moveNext();
    record = StringBuffer();
    expect(CsvParser(iter).parseField(record), throwsA(isA<FormatException>()));

    // A field with no closing character (',' or '\n') throws an AssertionError.
    iter = _iterFromLine('b');
    await iter.moveNext();
    record = StringBuffer();
    expect(CsvParser(iter).parseField(record), throwsA(isA<AssertionError>()));
  });

  test('test parseEscapedField', () async {
    StreamIterator<String> iter;
    StringBuffer record;

    // Escape quotes aren't added to record.
    iter = _iterFromLine('"a","b,c"\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseEscapedField(record);
    expect(await _remaining(iter), ',"b,c"\n');
    expect(record.toString(), 'a');

    // Parse empty escaped field.
    iter = _iterFromLine('"",b,c\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseEscapedField(record);
    expect(await _remaining(iter), ',b,c\n');
    expect(record.toString(), '');

    // Parse an escaped field with a comma.
    iter = _iterFromLine('"b,c"\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseEscapedField(record);
    expect(await iter.moveNext(), false);
    expect(record.toString(), 'b,c');

    // A properly escaped double quote is added to record.
    iter = _iterFromLine('"b""",c\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseEscapedField(record);
    expect(await _remaining(iter), ',c\n');
    expect(record.toString(), 'b"');

    // A properly escaped newline is added to record.
    iter = _iterFromLine('"b\n",c\n');
    await iter.moveNext();
    record = StringBuffer();
    await CsvParser(iter).parseEscapedField(record);
    expect(await _remaining(iter), ',c\n');
    expect(record.toString(), 'b\n');

    // A FormatException is thrown if there's a hanging escape quote.
    iter = _iterFromLine('"b,c\n');
    await iter.moveNext();
    record = StringBuffer();
    expect(() => CsvParser(iter).parseEscapedField(record),
        throwsA(isA<FormatException>()));
  });

  test('test parseEscapedField errors', () async {
    StreamIterator<String> iter;
    StringBuffer record;

    // A FormatException is thrown if there's a hanging escape quote.
    iter = _iterFromLine('"b,c\n');
    await iter.moveNext();
    record = StringBuffer();
    expect(() => CsvParser(iter).parseEscapedField(record),
        throwsA(isA<FormatException>()));
  });

  test('test parseLine', () async {
    StreamIterator<String> iter;

    // Parse a generic line with no escaping.
    iter = _iterFromLine('a,bc,def\nx,y,z\n');
    expect(await CsvParser(iter).parseLine(), <dynamic>['a', 'bc', 'def']);
    expect(await _remaining(iter), '\nx,y,z\n');

    // Parse a generic line with a blank final field.
    iter = _iterFromLine('a,b,\nx,y,z\n');
    expect(await CsvParser(iter).parseLine(), <dynamic>['a', 'b', '']);
    expect(await _remaining(iter), '\nx,y,z\n');

    // Parse a generic line with a blank internal field.
    iter = _iterFromLine('a,,c\nx,y,z\n');
    expect(await CsvParser(iter).parseLine(), <dynamic>['a', '', 'c']);
    expect(await _remaining(iter), '\nx,y,z\n');

    // Parse a line with basic escaping.
    iter = _iterFromLine('a,"bc","def"\nx,y,z\n');
    expect(await CsvParser(iter).parseLine(), <dynamic>['a', 'bc', 'def']);
    expect(await _remaining(iter), '\nx,y,z\n');

    // Parse a line with escaped commas.
    iter = _iterFromLine('a,"b,c","d,e,f"\nx,y,z\n');
    expect(await CsvParser(iter).parseLine(), <dynamic>['a', 'b,c', 'd,e,f']);
    expect(await _remaining(iter), '\nx,y,z\n');

    // Parse a line with escaped double quotes and commas.
    iter = _iterFromLine('a,"b""c","d,e,f"""\nx,y,z\n');
    expect(await CsvParser(iter).parseLine(), <dynamic>['a', 'b"c', 'd,e,f"']);
    expect(await _remaining(iter), '\nx,y,z\n');
  });

  test('test parseLine errors', () async {
    StreamIterator<String> iter;

    // Parse a line with an unclosed escape quote.
    iter = _iterFromLine('a,"b"",c\n');
    expect(CsvParser(iter).parseLine(), throwsA(isA<FormatException>()));

    // Parse a line containing a field without a closing newline.
    iter = _iterFromLine('a,b');
    expect(CsvParser(iter).parseLine(), throwsA(isA<AssertionError>()));

    // Parse a line containing an escaped field without a closing newline.
    iter = _iterFromLine('a,"b"');
    expect(CsvParser(iter).parseLine(), throwsA(isA<AssertionError>()));
  });
}
