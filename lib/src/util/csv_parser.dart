/// CSVParser parses a stream of lines into a list of vals compliant with
/// the CSV standard
///
/// See RFC4180 for details on CSV standard
class CsvParser {

  /// Takes a single line and parses it into a list of values according to the
  /// csv standard (see RFC4180)
  static List<String> parseLine(
      String line) {
    final records = <String>[];
    var i = 0;
    StringBuffer record;
    while (i < line.length) {
      record = StringBuffer();
      if (line[i] == "\"") {
        // if the csv field begins with a double quote, parse it with
        // proper character escaping - see RC4180 2.5-2.7
        i = parseEscapedField(line, record, i);
      } else {
        i = parseField(line, record, i);
      }
      records.add(record.toString());
      // increment past the current char (a comma or EOL)
      i++;
    }
    return records;
  }

  /// Parse and write chars to buff until a comma is reached, then return the
  /// the index after the last char consumed
  static int parseField(
      String line,
      StringBuffer record,
      int startIndex) {
    var i = startIndex;
    while (i < line.length && line[i] != ",") {
      if (line[i] == "\"") {
        throw ArgumentError("A field contained an unescaped double quote. "
            "See section 2.5 of https://tools.ietf.org/html/rfc4180.\n"
            "character $i of line:\n$line\n");
      }
      record.write(line[i]);
      i++;
    }
    return i;
  }

  /// Like _parseField, but with support for character escaping
  static int parseEscapedField(
      String line,
      StringBuffer record,
      int startIndex) {
    var i = startIndex;
    assert(line[i]=="\"", "parseEscapedField was called on an unescaped field at"
        " char $i of line $line");
    // increment past the first char (a double quote)
    i++;
    while (i < line.length) {
      if (line[i] == "\"") {
        if (i + 1 < line.length && line[i + 1] == "\"") {
          // A double quote preceded by a double quote is escaped - increment
          // past this double quote and write the next one to record
          i++;
        } else {
          // Single double quote, this is the end of the escaped sequence
          return i + 1;
        }
      }
      record.write(line[i]);
      i++;
    }
    // reached end of line without closing the escape quote
    throw ArgumentError("A field contained an escape quote without a closing escape quote. "
        "See section 2.5 of https://tools.ietf.org/html/rfc4180.\n"
        "character $i of line:\n$line\n");
  }
}
