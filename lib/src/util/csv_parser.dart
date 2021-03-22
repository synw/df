import 'dart:async';

/// CSVParser parses a stream of lines into a list of vals compliant with
/// the CSV standard.
///
/// See RFC4180 for details on CSV standard.
class CsvParser {
  final StreamIterator<String> _charIter;

  // Used for providing context in error messages.
  StringBuffer _lineSoFar = StringBuffer();

  /// Parses the characters from the given stream iterator as a csv (see RFC4180)
  /// and returns a list of rows where each row is a list of values.
  ///
  /// The strings supplied by [singleCharacterIterator] *must* be single element
  /// strings and the last character in the stream must be the newline character.
  CsvParser(StreamIterator<String> singleCharacterIterator)
      : _charIter = singleCharacterIterator;

  // Use this instead of `_charIter.moveNext()` to keep `_lineSoFar` in sync.
  Future<bool> _moveNext() async {
    final ret = await _charIter.moveNext();
    assert(
        _charIter.current == null || _charIter.current.length == 1,
        'Character stream produced a string longer than one character: '
        '${_charIter.current}');
    _lineSoFar.write(_charIter.current);
    return ret;
  }

  /// Takes a single line and parses it into a list of values according to the
  /// csv standard.
  ///
  /// If the iterator has run out of valid csv lines [parseLine] will return null.
  Future<List<String>?> parseLine() async {
    final records = <String>[];
    _lineSoFar = StringBuffer();
    while (await _moveNext()) {
      final record = StringBuffer();
      if (_charIter.current == '"') {
        // If the csv field begins with a double quote, parse it with
        // proper character escaping - see RC4180 2.5-2.7.
        await parseEscapedField(record);
      } else {
        await parseField(record);
      }
      records.add(record.toString());
      // Non empty lines should return before the while loop is complete.
      assert(
          _isDelimiter(_charIter.current),
          'A parsed field did not end in a delimiter. This should not happen '
          'and indicates a bug in df.dart.\n'
          'Please file a bug at:'
          'https://github.com/synw/df/issues\n'
          '$_getCurrentCharacterMessage\n');
      if (_charIter.current == '\n') {
        // Reached the end of the current csv line.
        return records;
      }
    }
    // This was the final newline at the end of file, return null.
    return null;
  }

  /// Parse and write chars to buff until a comma is reached.
  ///
  /// [ParseField] expects to be called on an iterator for which 'moveNext' has
  /// already been called.
  Future<void> parseField(StringBuffer record) async {
    _assertMoveNextHasBeenCalled();
    // Special case for the empty field.
    if (_isDelimiter(_charIter.current)) return;
    record.write(_charIter.current);
    while (await _moveNext()) {
      if (_isDelimiter(_charIter.current)) {
        // Reached end of field, exit
        return;
      }
      if (_charIter.current == '"') {
        throw FormatException('A field contained an unescaped double quote at: '
            'See section 2.5 of https://tools.ietf.org/html/rfc4180.\n'
            '$_getCurrentCharacterMessage\n');
      }
      record.write(_charIter.current);
    }
    // Function should return before the while loop completes.
    throw AssertionError(
        'A field was not terminated in either a comma or newline. This should '
        'not happen and indicates a bug in df.dart.\n'
        'Please file a bug at:'
        'https://github.com/synw/df/issues\n'
        '$_getCurrentCharacterMessage\n');
  }

  /// Like [parseField], but with support for character escaping.
  ///
  /// ParseField expects to be called on an iterator for which 'moveNext' has
  /// already been called.
  Future<void> parseEscapedField(StringBuffer record) async {
    // parseEscapedField expects to be called on an iterator for which 'moveNext'
    // has already been called.
    _assertMoveNextHasBeenCalled();
    assert(
        _charIter.current == '"',
        'parseEscapedField was called on an unescaped field.\n'
        '$_getCurrentCharacterMessage\n');
    while (await _moveNext()) {
      if (_charIter.current == '"') {
        // Step past the current double quote.
        await _moveNext();
        // Silly linter doesn't know about side-effects.
        // ignore: invariant_booleans
        if (_charIter.current != '"') {
          // Single double quote, this is the end of the escaped sequence.
          return;
        }
      }
      // The current character is either a regular character or an escaped
      // double quote-write it to record.
      record.write(_charIter.current);
    }
    // Reached end of line without closing the escape quote.
    throw FormatException(
        'A field contained an escape quote without a closing escape quote. '
        'See section 2.5 of https://tools.ietf.org/html/rfc4180.\n'
        '$_getCurrentCharacterMessage\n');
  }

  bool _isDelimiter(String char) => char == ',' || char == '\n';

  String get _getCurrentCharacterMessage =>
      'Error at character \'${_charIter.current}\' at position #${_lineSoFar.length - 1} of line:\n$_lineSoFar...';

  void _assertMoveNextHasBeenCalled() {
    // CharacterRange returns an empty String if moveNext hasn't been called yet.
    // This assert will also pass if the character range is empty.
    assert(_charIter.current != '',
        'You must call \'moveNext\' before calling \'parseField\'.');
  }
}
