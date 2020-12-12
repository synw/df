import 'package:jiffy/jiffy.dart';
import 'package:meta/meta.dart';

/// A data frame column
class DataFrameColumn {
  /// Provide a [name] and [type]
  DataFrameColumn({@required this.name, @required this.type});

  /// The column's name
  String name;

  /// The column's type
  Type type;

  /// Infer the column types from a datapoint
  DataFrameColumn.inferFromRecord(String record, this.name, {String dateFormat})
      : assert(name != null),
        assert(record != null) {
    type = String;
    if (int.tryParse(record) != null) {
      type = int;
    } else if (double.tryParse(record) != null) {
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
