import 'package:characters/characters.dart';

/// CSVParser parses a stream of lines into a list of vals compliant with
/// the CSV standard.
///
/// See RFC4180 for details on CSV standard.
class CsvParser {
  /// Takes a single line and parses it into a list of values according to the
  /// csv standard (see RFC4180).
  static List<String> parseLine(String line) {
    final records = <String>[];
    StringBuffer record;
    final charIter = line.characters.iterator;
    while (charIter.moveNext()) {
      record = StringBuffer();
      if (charIter.current == '"') {
        // If the csv field begins with a double quote, parse it with
        // proper character escaping - see RC4180 2.5-2.7.
        parseEscapedField(record, charIter);
      } else {
        parseField(record, charIter);
      }
      records.add(record.toString());
    }
    // special case for a line that ends with comma (ie a blank field).
    if (line[line.length - 1] == ',') records.add('');
    return records;
  }

  /// Parse and write chars to buff until a comma is reached, then return the
  /// the index after the last char consumed.
  static void parseField(StringBuffer record, CharacterRange charIter) {
    // parseField expects to be called on an iterator for which 'moveNext' has
    // already been called.
    CsvParser._assertMoveNextHasBeenCalled(charIter);
    // Need to include the current character in the field.
    record.write(charIter.current);
    while (charIter.moveNext() && charIter.current != ',') {
      if (charIter.current == '"') {
        throw FormatException('A field contained an unescaped double quote. '
            'See section 2.5 of https://tools.ietf.org/html/rfc4180.\n'
            'Character ${charIter.stringBefore.length} of line:\n${charIter.source}\n');
      }
      record.write(charIter.current);
    }
  }

  /// Like _parseField, but with support for character escaping.
  static void parseEscapedField(StringBuffer record, CharacterRange charIter) {
    // parseEscapedField expects to be called on an iterator for which 'moveNext'
    // has already been called.
    CsvParser._assertMoveNextHasBeenCalled(charIter);
    assert(
        charIter.current == '"',
        'parseEscapedField was called on an unescaped field at'
        ' char ${charIter.stringBefore.length} of line ${charIter.source}');
    while (charIter.moveNext()) {
      if (charIter.current == '"') {
        // Step past the current double quote.
        charIter.moveNext();
        // ignore: invariant_booleans
        if (charIter.current != '"') {
          // Single double quote, this is the end of the escaped sequence.
          return;
        }
      }
      // The current character is either a regular character or an escaped double quote-
      // write it to record.
      record.write(charIter.current);
    }
    // Reached end of line without closing the escape quote.
    throw FormatException(
        'A field contained an escape quote without a closing escape quote. '
        'See section 2.5 of https://tools.ietf.org/html/rfc4180.\n'
        'character ${charIter.stringBefore.length} of line:\n${charIter.source}\n');
  }

  static void _assertMoveNextHasBeenCalled(CharacterRange charIter) {
    // CharacterRange returns an empty String if moveNext hasn't been called yet.
    // This assert will also pass if the character range is empty.
    assert(charIter.current != '',
        'You must call \'moveNext\' before calling \'parseField\'.');
  }
}
