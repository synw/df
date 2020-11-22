import 'package:df/src/util/csv_parser.dart';
import 'package:test/test.dart';

void main() {
  test("test parseField", () {
    StringBuffer record;
    String line;

    line = "a,bc,def";
    // parse first field
    record = StringBuffer();
    expect(CsvParser.parseField(line, record, 0), 1);
    expect(record.toString(), "a");

    // parse second field with two chars
    record = StringBuffer();
    expect(CsvParser.parseField(line, record, 2), 4);
    expect(record.toString(), "bc");

    // parse final field
    record = StringBuffer();
    expect(CsvParser.parseField(line, record, 5), 8);
    expect(record.toString(), "def");

    // a double quote in an unescaped field throws an error
    record = StringBuffer();
    line = "a,b\",c";
    expect(() => CsvParser.parseField(line, record, 2), throwsA(isA<ArgumentError>()));
  });


  test("test parseEscapedField", () {
    StringBuffer record;
    String line;

    line = "\"a\",\"b,c\"";
    // escape quotes aren't added to record
    record = StringBuffer();
    expect(CsvParser.parseEscapedField(line, record, 0), 3);
    expect(record.toString(), "a");

    // parse an escaped field with a comma
    record = StringBuffer();
    expect(CsvParser.parseEscapedField(line, record, 4), 9);
    expect(record.toString(), "b,c");

    line = "a,\"b\"\"\",c";
    // A properly escaped double quote is added to record
    record = StringBuffer();
    expect(CsvParser.parseEscapedField(line, record, 2), 7);
    expect(record.toString(), "b\"");

    // An ArgumentError is thrown if there's a hanging preceding escape quote
    line = "a,\"b,c";
    record = StringBuffer();
    expect(() => CsvParser.parseEscapedField(line, record, 2), throwsA(isA<ArgumentError>()));
  });

  test("test parseLine", () {
    StringBuffer record;
    String line;

    // parse a generic line with no escaping
    line = "a,bc,def";
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>["a", "bc", "def"]);

    // parse a line with basic escaping
    line = "a,\"bc\",\"def\"";
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>["a", "bc", "def"]);

    // parse a line with escaped commas
    line = "a,\"b,c\",\"d,e,f\"";
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>["a", "b,c", "d,e,f"]);

    // parse a line with escaped double quotes and commas
    line = "a,\"b\"\"c\",\"d,e,f\"\"\"";
    record = StringBuffer();
    expect(CsvParser.parseLine(line), <dynamic>["a", "b\"c", "d,e,f\""]);

    // parse a line with an unclosed escape quote
    line = "a,\"b\"\",c";
    record = StringBuffer();
    expect(() => CsvParser.parseLine(line), throwsA(isA<ArgumentError>()));
  });
}