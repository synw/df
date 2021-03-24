import 'package:jiffy/jiffy.dart';

/// A data frame column
class DataFrameColumn {
  /// Provide a [name] and [type]
  DataFrameColumn({required this.name, required this.type});

  /// The column's name
  final String name;

  /// The column's type
  Type type;

  /// Infer the column types from a datapoint.
  ///
  /// If a field contains whitespace and/or newlines it will not be treated as a
  /// numeric type even if it could be successfully parsed as an int or double.
  ///
  /// eg '\n23' and ' 23' will evaluate as strings and retain their newline and
  /// space respectively.
  DataFrameColumn.inferFromRecord(String record, this.name,
      {String? dateFormat})
      : type = String {
    // tryParse will ignore whitespace, but values with white space should be
    // treated as strings.
    if (!record.contains(RegExp('[\s\n]')) && int.tryParse(record) != null) {
      type = int;
    } else if (!record.contains(RegExp('[\s\n]')) &&
        double.tryParse(record) != null) {
      type = double;
    } else {
      try {
        if (dateFormat != null) {
          Jiffy(record.toString(), dateFormat);
          type = DateTime;
        } else {
          final d = DateTime.tryParse(record.toString());
          if (d != null) {
            type = DateTime;
          }
        }
      } catch (_) {
        type = String;
      }
    }
  }

  @override
  String toString() {
    return '$name ($type)';
  }

  @override
  int get hashCode => name.hashCode;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is DataFrameColumn &&
          runtimeType == other.runtimeType &&
          name == other.name;
}
