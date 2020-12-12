import 'dart:math';

import 'package:characters/characters.dart';
import 'package:df/src/util/csv_parser.dart';
import 'package:test/test.dart';

void main() {
  test('test parseField', () {
    CharacterRange iter;
    StringBuffer record;

    // Parse first field.
    iter = 'a,bc,def'.characters.iterator..moveNext();
    record = StringBuffer();
    CsvParser.parseField(record, iter);
    expect(iter.current + iter.stringAfter, ',bc,def');
    expect(record.toString(), 'a');

    // Parse second field with two chars.
    iter = 'bc,def'.characters.iterator..moveNext();
    record = StringBuffer();
    CsvParser.parseField(record, iter);
    expect(iter.current + iter.stringAfter, ',def');
    expect(record.toString(), 'bc');

    // Parse final field.
    iter = 'def'.characters.iterator..moveNext();
    record = StringBuffer();
    CsvParser.parseField(record, iter);
    expect(iter.isEmpty, true);
    expect(record.toString(), 'def');

    // A double quote in an unescaped field throws an error.
    iter = 'b"'.characters.iterator..moveNext();
    record = StringBuffer();
    expect(() => CsvParser.parseField(record, iter),
        throwsA(isA<FormatException>()));
  });

  test('test parseEscapedField', () {
    CharacterRange iter;
    StringBuffer record;

    // Escape quotes aren't added to record.
    iter = '"a","b,c"'.characters.iterator..moveNext();
    record = StringBuffer();
    CsvParser.parseEscapedField(record, iter);
    expect(iter.current + iter.stringAfter, ',"b,c"');
    expect(record.toString(), 'a');

    // Parse an escaped field with a comma.
    iter = '"b,c"'.characters.iterator..moveNext();
    record = StringBuffer();
    CsvParser.parseEscapedField(record, iter);
    expect(iter.isEmpty, true);
    expect(record.toString(), 'b,c');

    // A properly escaped double quote is added to record.
    iter = '"b""",c'.characters.iterator..moveNext();
    record = StringBuffer();
    CsvParser.parseEscapedField(record, iter);
    expect(iter.current + iter.stringAfter, ',c');
    expect(record.toString(), 'b"');

    // A FormatException is thrown if there's a hanging escape quote.
    iter = '"b,c'.characters.iterator..moveNext();
    record = StringBuffer();
    expect(() => CsvParser.parseEscapedField(record, iter),
        throwsA(isA<FormatException>()));
  });

  test('test parseLine', () {
    StringBuffer record;
    String line;

    // Parse a generic line with no escaping.
    line = 'a,bc,def';
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>['a', 'bc', 'def']);

    // Parse a generic line with a blank final field.
    line = 'a,b,';
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>['a', 'b', '']);

    // Parse a line with basic escaping.
    line = 'a,"bc","def"';
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>['a', 'bc', 'def']);

    // Parse a line with escaped commas.
    line = 'a,"b,c","d,e,f"';
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>['a', 'b,c', 'd,e,f']);

    // Parse a line with escaped double quotes and commas.
    line = 'a,"b""c","d,e,f"""';
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>['a', 'b"c', 'd,e,f"']);

    // Parse a line with an unclosed escape quote.
    line = 'a,"b"",c';
    record = StringBuffer();
    expect(() => CsvParser.parseLine(line), throwsA(isA<FormatException>()));
  });
}
