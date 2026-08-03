//
//  Generated code. Do not modify.
//  source: tracking/v1/query.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:core' as $core;

import 'package:protobuf/protobuf.dart' as $pb;

class Op extends $pb.ProtobufEnum {
  static const Op OP_UNSPECIFIED = Op._(0, _omitEnumNames ? '' : 'OP_UNSPECIFIED');
  static const Op OP_EQ = Op._(1, _omitEnumNames ? '' : 'OP_EQ');
  static const Op OP_NEQ = Op._(2, _omitEnumNames ? '' : 'OP_NEQ');
  static const Op OP_IN = Op._(3, _omitEnumNames ? '' : 'OP_IN');
  static const Op OP_GT = Op._(4, _omitEnumNames ? '' : 'OP_GT');
  static const Op OP_LT = Op._(5, _omitEnumNames ? '' : 'OP_LT');

  static const $core.List<Op> values = <Op> [
    OP_UNSPECIFIED,
    OP_EQ,
    OP_NEQ,
    OP_IN,
    OP_GT,
    OP_LT,
  ];

  static final $core.Map<$core.int, Op> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Op? valueOf($core.int value) => _byValue[value];

  const Op._($core.int v, $core.String n) : super(v, n);
}

class Interval extends $pb.ProtobufEnum {
  static const Interval INTERVAL_UNSPECIFIED = Interval._(0, _omitEnumNames ? '' : 'INTERVAL_UNSPECIFIED');
  static const Interval INTERVAL_HOUR = Interval._(1, _omitEnumNames ? '' : 'INTERVAL_HOUR');
  static const Interval INTERVAL_DAY = Interval._(2, _omitEnumNames ? '' : 'INTERVAL_DAY');
  static const Interval INTERVAL_WEEK = Interval._(3, _omitEnumNames ? '' : 'INTERVAL_WEEK');

  static const $core.List<Interval> values = <Interval> [
    INTERVAL_UNSPECIFIED,
    INTERVAL_HOUR,
    INTERVAL_DAY,
    INTERVAL_WEEK,
  ];

  static final $core.Map<$core.int, Interval> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Interval? valueOf($core.int value) => _byValue[value];

  const Interval._($core.int v, $core.String n) : super(v, n);
}

class Metric extends $pb.ProtobufEnum {
  static const Metric METRIC_UNSPECIFIED = Metric._(0, _omitEnumNames ? '' : 'METRIC_UNSPECIFIED');
  static const Metric METRIC_EVENTS = Metric._(1, _omitEnumNames ? '' : 'METRIC_EVENTS');
  static const Metric METRIC_USERS = Metric._(2, _omitEnumNames ? '' : 'METRIC_USERS');

  static const $core.List<Metric> values = <Metric> [
    METRIC_UNSPECIFIED,
    METRIC_EVENTS,
    METRIC_USERS,
  ];

  static final $core.Map<$core.int, Metric> _byValue = $pb.ProtobufEnum.initByValue(values);
  static Metric? valueOf($core.int value) => _byValue[value];

  const Metric._($core.int v, $core.String n) : super(v, n);
}


const _omitEnumNames = $core.bool.fromEnvironment('protobuf.omit_enum_names');
