/// An exception for a column not found
class ColumnNotFoundException implements Exception {
  /// Default construtor
  ColumnNotFoundException(this.message);

  /// The error message
  final String message;
}

/// An exception for a file not found
class FileNotFoundException implements Exception {
  /// Default construtor
  FileNotFoundException(this.message);

  /// The error message
  final String message;
}
