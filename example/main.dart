import 'package:df/df.dart';

Future<void> main() async {
  final df = await DataFrame.fromCsv('dataset/stocks.csv',
      dateFormat: 'MMM dd yyyy', verbose: true);
  df.show();
  print(df.columns);
}
