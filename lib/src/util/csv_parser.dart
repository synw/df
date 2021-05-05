import 'dart:async';

/// CSVParser parses a stream of lines into a list of vals compliant with
/// the CSV standard.
///
/// See RFC4180 for details on CSV standard.
class CsvParser {
  final CharIter _charIter;

  /// Parses the characters from the given stream iterator as a csv (see RFC4180)
  /// and returns a list of rows where each row is a list of values.
  ///
  /// The strings supplied by [singleCharacterIterator] *must* be single element
  /// strings and the last character in the stream must be the newline character.
  CsvParser(this._charIter);

  /// Takes a single line and parses it into a list of values according to the
  /// csv standard.
  ///
  /// If the iterator has run out of valid csv lines [parseLine] will return null.
  Future<List<String>?> parseLine() async {
    final records = <String>[];
    _charIter.resetLine();
    while (await _charIter.moveNext()) {
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
          '${_charIter.currentErrorMessage}\n');
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
    while (await _charIter.moveNext()) {
      if (_isDelimiter(_charIter.current)) {
        // Reached end of field, exit
        return;
      }
      if (_charIter.current == '"') {
        throw FormatException('A field contained an unescaped double quote at: '
            'See section 2.5 of https://tools.ietf.org/html/rfc4180.\n'
            '${_charIter.currentErrorMessage}\n');
      }
      record.write(_charIter.current);
    }
    // Function should return before the while loop completes.
    throw AssertionError(
        'A field was not terminated in either a comma or newline. This should '
        'not happen and indicates a bug in df.dart.\n'
        'Please file a bug at:'
        'https://github.com/synw/df/issues\n'
        '${_charIter.currentErrorMessage}\n');
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
        '${_charIter.currentErrorMessage}\n');
    while (await _charIter.moveNext()) {
      if (_charIter.current == '"') {
        // Step past the current double quote.
        await _charIter.moveNext();
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
        '${_charIter.currentErrorMessage}\n');
  }

  bool _isDelimiter(String? char) => char == ',' || char == '\n';

  void _assertMoveNextHasBeenCalled() {
    // CharacterRange returns an empty String if moveNext hasn't been called yet.
    // This assert will also pass if the character range is empty.
    assert(_charIter.current != '',
        'You must call \'moveNext\' before calling \'parseField\'.');
  }
}

/// This helper class returns null if the stream is complete and keeps track of
/// each the csv line as it's seen so far for constructing debug error messages.
class CharIter {
  final StreamIterator<String> _iter;

  /// The iterators current character.
  String? current;

  /// Construct a character iterator from a StreamIterator.
  CharIter(Stream<String> iter) : _iter = StreamIterator(iter);

  /// The characters parsed from the current line so far. Used for producing
  /// debug messages.
  final StringBuffer _lineSoFar = StringBuffer();

  /// Produce an error message corresponding to the current line.
  String get currentErrorMessage =>
      'Error at character \'$current\' at position #${_lineSoFar.length - 1} of line:\n$_lineSoFar...';

  /// Reset the debug message when a new line is reached.
  void resetLine() => _lineSoFar.clear();

  /// Move to the next character in the iterator. Returns false and sets current
  /// to null if the end of the iterator is reached.
  Future<bool> moveNext() async {
    final ret = await _iter.moveNext();
    if (ret) {
      assert(
          _iter.current.length == 1,
          'Character stream produced a string longer than one character: '
          '${_iter.current}');
      current = _iter.current;
      _lineSoFar.write(current);
    } else {
      current = null;
    }
    return ret;
  }
}
