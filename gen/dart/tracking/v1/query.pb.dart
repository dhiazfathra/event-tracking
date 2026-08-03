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

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'query.pbenum.dart';

export 'query.pbenum.dart';

class Filter extends $pb.GeneratedMessage {
  factory Filter({
    $core.String? field_1,
    Op? op,
    $core.Iterable<$core.String>? values,
  }) {
    final $result = create();
    if (field_1 != null) {
      $result.field_1 = field_1;
    }
    if (op != null) {
      $result.op = op;
    }
    if (values != null) {
      $result.values.addAll(values);
    }
    return $result;
  }
  Filter._() : super();
  factory Filter.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Filter.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Filter', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'field')
    ..e<Op>(2, _omitFieldNames ? '' : 'op', $pb.PbFieldType.OE, defaultOrMaker: Op.OP_UNSPECIFIED, valueOf: Op.valueOf, enumValues: Op.values)
    ..pPS(3, _omitFieldNames ? '' : 'values')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Filter clone() => Filter()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Filter copyWith(void Function(Filter) updates) => super.copyWith((message) => updates(message as Filter)) as Filter;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Filter create() => Filter._();
  Filter createEmptyInstance() => create();
  static $pb.PbList<Filter> createRepeated() => $pb.PbList<Filter>();
  @$core.pragma('dart2js:noInline')
  static Filter getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Filter>(create);
  static Filter? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get field_1 => $_getSZ(0);
  @$pb.TagNumber(1)
  set field_1($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasField_1() => $_has(0);
  @$pb.TagNumber(1)
  void clearField_1() => clearField(1);

  @$pb.TagNumber(2)
  Op get op => $_getN(1);
  @$pb.TagNumber(2)
  set op(Op v) { setField(2, v); }
  @$pb.TagNumber(2)
  $core.bool hasOp() => $_has(1);
  @$pb.TagNumber(2)
  void clearOp() => clearField(2);

  @$pb.TagNumber(3)
  $core.List<$core.String> get values => $_getList(2);
}

class TimeseriesRequest extends $pb.GeneratedMessage {
  factory TimeseriesRequest({
    $core.String? eventName,
    $fixnum.Int64? fromMs,
    $fixnum.Int64? toMs,
    Interval? interval,
    Metric? metric,
    $core.Iterable<Filter>? filters,
    $core.Iterable<$core.String>? groupBy,
    $core.bool? approximate,
  }) {
    final $result = create();
    if (eventName != null) {
      $result.eventName = eventName;
    }
    if (fromMs != null) {
      $result.fromMs = fromMs;
    }
    if (toMs != null) {
      $result.toMs = toMs;
    }
    if (interval != null) {
      $result.interval = interval;
    }
    if (metric != null) {
      $result.metric = metric;
    }
    if (filters != null) {
      $result.filters.addAll(filters);
    }
    if (groupBy != null) {
      $result.groupBy.addAll(groupBy);
    }
    if (approximate != null) {
      $result.approximate = approximate;
    }
    return $result;
  }
  TimeseriesRequest._() : super();
  factory TimeseriesRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory TimeseriesRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'TimeseriesRequest', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'eventName')
    ..aInt64(2, _omitFieldNames ? '' : 'fromMs')
    ..aInt64(3, _omitFieldNames ? '' : 'toMs')
    ..e<Interval>(4, _omitFieldNames ? '' : 'interval', $pb.PbFieldType.OE, defaultOrMaker: Interval.INTERVAL_UNSPECIFIED, valueOf: Interval.valueOf, enumValues: Interval.values)
    ..e<Metric>(5, _omitFieldNames ? '' : 'metric', $pb.PbFieldType.OE, defaultOrMaker: Metric.METRIC_UNSPECIFIED, valueOf: Metric.valueOf, enumValues: Metric.values)
    ..pc<Filter>(6, _omitFieldNames ? '' : 'filters', $pb.PbFieldType.PM, subBuilder: Filter.create)
    ..pPS(7, _omitFieldNames ? '' : 'groupBy')
    ..aOB(8, _omitFieldNames ? '' : 'approximate')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  TimeseriesRequest clone() => TimeseriesRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  TimeseriesRequest copyWith(void Function(TimeseriesRequest) updates) => super.copyWith((message) => updates(message as TimeseriesRequest)) as TimeseriesRequest;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TimeseriesRequest create() => TimeseriesRequest._();
  TimeseriesRequest createEmptyInstance() => create();
  static $pb.PbList<TimeseriesRequest> createRepeated() => $pb.PbList<TimeseriesRequest>();
  @$core.pragma('dart2js:noInline')
  static TimeseriesRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TimeseriesRequest>(create);
  static TimeseriesRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get eventName => $_getSZ(0);
  @$pb.TagNumber(1)
  set eventName($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasEventName() => $_has(0);
  @$pb.TagNumber(1)
  void clearEventName() => clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get fromMs => $_getI64(1);
  @$pb.TagNumber(2)
  set fromMs($fixnum.Int64 v) { $_setInt64(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasFromMs() => $_has(1);
  @$pb.TagNumber(2)
  void clearFromMs() => clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get toMs => $_getI64(2);
  @$pb.TagNumber(3)
  set toMs($fixnum.Int64 v) { $_setInt64(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasToMs() => $_has(2);
  @$pb.TagNumber(3)
  void clearToMs() => clearField(3);

  @$pb.TagNumber(4)
  Interval get interval => $_getN(3);
  @$pb.TagNumber(4)
  set interval(Interval v) { setField(4, v); }
  @$pb.TagNumber(4)
  $core.bool hasInterval() => $_has(3);
  @$pb.TagNumber(4)
  void clearInterval() => clearField(4);

  @$pb.TagNumber(5)
  Metric get metric => $_getN(4);
  @$pb.TagNumber(5)
  set metric(Metric v) { setField(5, v); }
  @$pb.TagNumber(5)
  $core.bool hasMetric() => $_has(4);
  @$pb.TagNumber(5)
  void clearMetric() => clearField(5);

  @$pb.TagNumber(6)
  $core.List<Filter> get filters => $_getList(5);

  @$pb.TagNumber(7)
  $core.List<$core.String> get groupBy => $_getList(6);

  @$pb.TagNumber(8)
  $core.bool get approximate => $_getBF(7);
  @$pb.TagNumber(8)
  set approximate($core.bool v) { $_setBool(7, v); }
  @$pb.TagNumber(8)
  $core.bool hasApproximate() => $_has(7);
  @$pb.TagNumber(8)
  void clearApproximate() => clearField(8);
}

class Point extends $pb.GeneratedMessage {
  factory Point({
    $fixnum.Int64? bucketMs,
    $fixnum.Int64? value,
  }) {
    final $result = create();
    if (bucketMs != null) {
      $result.bucketMs = bucketMs;
    }
    if (value != null) {
      $result.value = value;
    }
    return $result;
  }
  Point._() : super();
  factory Point.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Point.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Point', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aInt64(1, _omitFieldNames ? '' : 'bucketMs')
    ..a<$fixnum.Int64>(2, _omitFieldNames ? '' : 'value', $pb.PbFieldType.OU6, defaultOrMaker: $fixnum.Int64.ZERO)
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Point clone() => Point()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Point copyWith(void Function(Point) updates) => super.copyWith((message) => updates(message as Point)) as Point;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Point create() => Point._();
  Point createEmptyInstance() => create();
  static $pb.PbList<Point> createRepeated() => $pb.PbList<Point>();
  @$core.pragma('dart2js:noInline')
  static Point getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Point>(create);
  static Point? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get bucketMs => $_getI64(0);
  @$pb.TagNumber(1)
  set bucketMs($fixnum.Int64 v) { $_setInt64(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasBucketMs() => $_has(0);
  @$pb.TagNumber(1)
  void clearBucketMs() => clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get value => $_getI64(1);
  @$pb.TagNumber(2)
  set value($fixnum.Int64 v) { $_setInt64(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasValue() => $_has(1);
  @$pb.TagNumber(2)
  void clearValue() => clearField(2);
}

class Series extends $pb.GeneratedMessage {
  factory Series({
    $core.Map<$core.String, $core.String>? group,
    $core.Iterable<Point>? points,
  }) {
    final $result = create();
    if (group != null) {
      $result.group.addAll(group);
    }
    if (points != null) {
      $result.points.addAll(points);
    }
    return $result;
  }
  Series._() : super();
  factory Series.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Series.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Series', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..m<$core.String, $core.String>(1, _omitFieldNames ? '' : 'group', entryClassName: 'Series.GroupEntry', keyFieldType: $pb.PbFieldType.OS, valueFieldType: $pb.PbFieldType.OS, packageName: const $pb.PackageName('tracking.v1'))
    ..pc<Point>(2, _omitFieldNames ? '' : 'points', $pb.PbFieldType.PM, subBuilder: Point.create)
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Series clone() => Series()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Series copyWith(void Function(Series) updates) => super.copyWith((message) => updates(message as Series)) as Series;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Series create() => Series._();
  Series createEmptyInstance() => create();
  static $pb.PbList<Series> createRepeated() => $pb.PbList<Series>();
  @$core.pragma('dart2js:noInline')
  static Series getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Series>(create);
  static Series? _defaultInstance;

  @$pb.TagNumber(1)
  $core.Map<$core.String, $core.String> get group => $_getMap(0);

  @$pb.TagNumber(2)
  $core.List<Point> get points => $_getList(1);
}

class TimeseriesResponse extends $pb.GeneratedMessage {
  factory TimeseriesResponse({
    $core.Iterable<Series>? series,
    $core.String? source,
    $core.bool? approximate,
    $fixnum.Int64? computedAt,
    $core.String? etag,
  }) {
    final $result = create();
    if (series != null) {
      $result.series.addAll(series);
    }
    if (source != null) {
      $result.source = source;
    }
    if (approximate != null) {
      $result.approximate = approximate;
    }
    if (computedAt != null) {
      $result.computedAt = computedAt;
    }
    if (etag != null) {
      $result.etag = etag;
    }
    return $result;
  }
  TimeseriesResponse._() : super();
  factory TimeseriesResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory TimeseriesResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'TimeseriesResponse', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..pc<Series>(1, _omitFieldNames ? '' : 'series', $pb.PbFieldType.PM, subBuilder: Series.create)
    ..aOS(2, _omitFieldNames ? '' : 'source')
    ..aOB(3, _omitFieldNames ? '' : 'approximate')
    ..aInt64(4, _omitFieldNames ? '' : 'computedAt')
    ..aOS(5, _omitFieldNames ? '' : 'etag')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  TimeseriesResponse clone() => TimeseriesResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  TimeseriesResponse copyWith(void Function(TimeseriesResponse) updates) => super.copyWith((message) => updates(message as TimeseriesResponse)) as TimeseriesResponse;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TimeseriesResponse create() => TimeseriesResponse._();
  TimeseriesResponse createEmptyInstance() => create();
  static $pb.PbList<TimeseriesResponse> createRepeated() => $pb.PbList<TimeseriesResponse>();
  @$core.pragma('dart2js:noInline')
  static TimeseriesResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TimeseriesResponse>(create);
  static TimeseriesResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.List<Series> get series => $_getList(0);

  @$pb.TagNumber(2)
  $core.String get source => $_getSZ(1);
  @$pb.TagNumber(2)
  set source($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasSource() => $_has(1);
  @$pb.TagNumber(2)
  void clearSource() => clearField(2);

  @$pb.TagNumber(3)
  $core.bool get approximate => $_getBF(2);
  @$pb.TagNumber(3)
  set approximate($core.bool v) { $_setBool(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasApproximate() => $_has(2);
  @$pb.TagNumber(3)
  void clearApproximate() => clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get computedAt => $_getI64(3);
  @$pb.TagNumber(4)
  set computedAt($fixnum.Int64 v) { $_setInt64(3, v); }
  @$pb.TagNumber(4)
  $core.bool hasComputedAt() => $_has(3);
  @$pb.TagNumber(4)
  void clearComputedAt() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get etag => $_getSZ(4);
  @$pb.TagNumber(5)
  set etag($core.String v) { $_setString(4, v); }
  @$pb.TagNumber(5)
  $core.bool hasEtag() => $_has(4);
  @$pb.TagNumber(5)
  void clearEtag() => clearField(5);
}


const _omitFieldNames = $core.bool.fromEnvironment('protobuf.omit_field_names');
const _omitMessageNames = $core.bool.fromEnvironment('protobuf.omit_message_names');
