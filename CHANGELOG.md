# Changelog

## 0.4.0-nullsafety.0

 - Updates df to support null safety.
 - `DataFrame.meanCol` and `DataMatrix.mean_` now require you to specify how to treat nulls
 -`DataFrame.sort` and `DataFrame.sort_` now require you to either specify how to treat nulls or to provide a custom sort function
 - All methods that previously accepted or returned `dynamic` now return `Object` or `Object?`

 All DataFrame columns have nullable data types. A future update may support non-nullable columns-if you'd like this feature comment and/or thumbs up it's corresponding [github issue](https://github.com/synw/df/issues/11)

## 0.3.0

Updates to support the quote escaping sections of the CSV standard (see: https://tools.ietf.org/html/rfc4180 sections 2.5-2.7):

 - supports using escape quotes to escape commas
 - supports using escape quotes to escape double quotes
 - supports using escape quotes to escape newlines

BREAKING CHANGE TO THE DF PACKAGE. Here are the breaking changes, with examples:

    Double quotes around a field are now consumed
    example csv input row: a,"b",c\n
    current df output values: {'a', '"b"', 'c'}
    df output values after PR: {'a', 'b', 'c'}
    An un-escaped double quote or an escaping double quote without a closing escape quote will throw an exception
    example csv input row: a,b"c,d\n
    current df output: {'a', 'b"c', 'd'}
    df output values after PR: FormatException('A field contained an escape...') <- explanatory error message is included
    Numeric records accompanied by whitespace will be inferred as Strings with whitespace
    example csv input row: a, 1 \n
    current df output values: {'a', 1} <--- parsed as int
    df output values after PR: {'a', ' 1 '} <---- remains a string


## 0.2.1

- Improve the tests
- Improve the docs

## 0.2.0

- Add support for timestamp formats
- Update dependencies

## 0.1.0

Initial