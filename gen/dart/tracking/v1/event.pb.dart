//
//  Generated code. Do not modify.
//  source: tracking/v1/event.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

enum Value_Kind {
  stringValue, 
  numberValue, 
  boolValue, 
  notSet
}

/// Value is the property union. Deliberately closed: props are analytics
/// dimensions, not arbitrary documents. Nested objects would make the
/// ClickHouse JSON subcolumn explosion unbounded.
class Value extends $pb.GeneratedMessage {
  factory Value({
    $core.String? stringValue,
    $core.double? numberValue,
    $core.bool? boolValue,
  }) {
    final $result = create();
    if (stringValue != null) {
      $result.stringValue = stringValue;
    }
    if (numberValue != null) {
      $result.numberValue = numberValue;
    }
    if (boolValue != null) {
      $result.boolValue = boolValue;
    }
    return $result;
  }
  Value._() : super();
  factory Value.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Value.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static const $core.Map<$core.int, Value_Kind> _Value_KindByTag = {
    1 : Value_Kind.stringValue,
    2 : Value_Kind.numberValue,
    3 : Value_Kind.boolValue,
    0 : Value_Kind.notSet
  };
  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Value', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..oo(0, [1, 2, 3])
    ..aOS(1, _omitFieldNames ? '' : 'stringValue')
    ..a<$core.double>(2, _omitFieldNames ? '' : 'numberValue', $pb.PbFieldType.OD)
    ..aOB(3, _omitFieldNames ? '' : 'boolValue')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Value clone() => Value()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Value copyWith(void Function(Value) updates) => super.copyWith((message) => updates(message as Value)) as Value;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Value create() => Value._();
  Value createEmptyInstance() => create();
  static $pb.PbList<Value> createRepeated() => $pb.PbList<Value>();
  @$core.pragma('dart2js:noInline')
  static Value getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Value>(create);
  static Value? _defaultInstance;

  Value_Kind whichKind() => _Value_KindByTag[$_whichOneof(0)]!;
  void clearKind() => clearField($_whichOneof(0));

  @$pb.TagNumber(1)
  $core.String get stringValue => $_getSZ(0);
  @$pb.TagNumber(1)
  set stringValue($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasStringValue() => $_has(0);
  @$pb.TagNumber(1)
  void clearStringValue() => clearField(1);

  @$pb.TagNumber(2)
  $core.double get numberValue => $_getN(1);
  @$pb.TagNumber(2)
  set numberValue($core.double v) { $_setDouble(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasNumberValue() => $_has(1);
  @$pb.TagNumber(2)
  void clearNumberValue() => clearField(2);

  @$pb.TagNumber(3)
  $core.bool get boolValue => $_getBF(2);
  @$pb.TagNumber(3)
  set boolValue($core.bool v) { $_setBool(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasBoolValue() => $_has(2);
  @$pb.TagNumber(3)
  void clearBoolValue() => clearField(3);
}

/// Context is SDK-populated, not host-app populated.
class Context extends $pb.GeneratedMessage {
  factory Context({
    $core.String? sdkVersion,
    $core.String? appVersion,
    $core.String? os,
    $core.String? osVersion,
    $core.String? locale,
  }) {
    final $result = create();
    if (sdkVersion != null) {
      $result.sdkVersion = sdkVersion;
    }
    if (appVersion != null) {
      $result.appVersion = appVersion;
    }
    if (os != null) {
      $result.os = os;
    }
    if (osVersion != null) {
      $result.osVersion = osVersion;
    }
    if (locale != null) {
      $result.locale = locale;
    }
    return $result;
  }
  Context._() : super();
  factory Context.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Context.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Context', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'sdkVersion')
    ..aOS(2, _omitFieldNames ? '' : 'appVersion')
    ..aOS(3, _omitFieldNames ? '' : 'os')
    ..aOS(4, _omitFieldNames ? '' : 'osVersion')
    ..aOS(5, _omitFieldNames ? '' : 'locale')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Context clone() => Context()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Context copyWith(void Function(Context) updates) => super.copyWith((message) => updates(message as Context)) as Context;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Context create() => Context._();
  Context createEmptyInstance() => create();
  static $pb.PbList<Context> createRepeated() => $pb.PbList<Context>();
  @$core.pragma('dart2js:noInline')
  static Context getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Context>(create);
  static Context? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get sdkVersion => $_getSZ(0);
  @$pb.TagNumber(1)
  set sdkVersion($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasSdkVersion() => $_has(0);
  @$pb.TagNumber(1)
  void clearSdkVersion() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get appVersion => $_getSZ(1);
  @$pb.TagNumber(2)
  set appVersion($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasAppVersion() => $_has(1);
  @$pb.TagNumber(2)
  void clearAppVersion() => clearField(2);

  @$pb.TagNumber(3)
  $core.String get os => $_getSZ(2);
  @$pb.TagNumber(3)
  set os($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasOs() => $_has(2);
  @$pb.TagNumber(3)
  void clearOs() => clearField(3);

  @$pb.TagNumber(4)
  $core.String get osVersion => $_getSZ(3);
  @$pb.TagNumber(4)
  set osVersion($core.String v) { $_setString(3, v); }
  @$pb.TagNumber(4)
  $core.bool hasOsVersion() => $_has(3);
  @$pb.TagNumber(4)
  void clearOsVersion() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get locale => $_getSZ(4);
  @$pb.TagNumber(5)
  set locale($core.String v) { $_setString(4, v); }
  @$pb.TagNumber(5)
  $core.bool hasLocale() => $_has(4);
  @$pb.TagNumber(5)
  void clearLocale() => clearField(5);
}

///  Event is the ingestion envelope.
///
///  tenant_id is deliberately absent. It is read server-side from the verified
///  token's tenant_id claim. A client-supplied tenant field is a cross-tenant
///  write primitive.
class Event extends $pb.GeneratedMessage {
  factory Event({
    $core.String? eventId,
    $core.String? name,
    $fixnum.Int64? tsClient,
    $fixnum.Int64? seq,
    $core.String? deviceId,
    $core.String? sessionId,
    $core.String? anonymousId,
    $core.String? userId,
    $core.Map<$core.String, Value>? props,
    Context? context,
  }) {
    final $result = create();
    if (eventId != null) {
      $result.eventId = eventId;
    }
    if (name != null) {
      $result.name = name;
    }
    if (tsClient != null) {
      $result.tsClient = tsClient;
    }
    if (seq != null) {
      $result.seq = seq;
    }
    if (deviceId != null) {
      $result.deviceId = deviceId;
    }
    if (sessionId != null) {
      $result.sessionId = sessionId;
    }
    if (anonymousId != null) {
      $result.anonymousId = anonymousId;
    }
    if (userId != null) {
      $result.userId = userId;
    }
    if (props != null) {
      $result.props.addAll(props);
    }
    if (context != null) {
      $result.context = context;
    }
    return $result;
  }
  Event._() : super();
  factory Event.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Event.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Event', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'eventId')
    ..aOS(2, _omitFieldNames ? '' : 'name')
    ..aInt64(3, _omitFieldNames ? '' : 'tsClient')
    ..a<$fixnum.Int64>(4, _omitFieldNames ? '' : 'seq', $pb.PbFieldType.OU6, defaultOrMaker: $fixnum.Int64.ZERO)
    ..aOS(5, _omitFieldNames ? '' : 'deviceId')
    ..aOS(6, _omitFieldNames ? '' : 'sessionId')
    ..aOS(7, _omitFieldNames ? '' : 'anonymousId')
    ..aOS(8, _omitFieldNames ? '' : 'userId')
    ..m<$core.String, Value>(9, _omitFieldNames ? '' : 'props', entryClassName: 'Event.PropsEntry', keyFieldType: $pb.PbFieldType.OS, valueFieldType: $pb.PbFieldType.OM, valueCreator: Value.create, valueDefaultOrMaker: Value.getDefault, packageName: const $pb.PackageName('tracking.v1'))
    ..aOM<Context>(10, _omitFieldNames ? '' : 'context', subBuilder: Context.create)
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Event clone() => Event()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Event copyWith(void Function(Event) updates) => super.copyWith((message) => updates(message as Event)) as Event;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Event create() => Event._();
  Event createEmptyInstance() => create();
  static $pb.PbList<Event> createRepeated() => $pb.PbList<Event>();
  @$core.pragma('dart2js:noInline')
  static Event getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Event>(create);
  static Event? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get eventId => $_getSZ(0);
  @$pb.TagNumber(1)
  set eventId($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasEventId() => $_has(0);
  @$pb.TagNumber(1)
  void clearEventId() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get name => $_getSZ(1);
  @$pb.TagNumber(2)
  set name($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasName() => $_has(1);
  @$pb.TagNumber(2)
  void clearName() => clearField(2);

  @$pb.TagNumber(3)
  $fixnum.Int64 get tsClient => $_getI64(2);
  @$pb.TagNumber(3)
  set tsClient($fixnum.Int64 v) { $_setInt64(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasTsClient() => $_has(2);
  @$pb.TagNumber(3)
  void clearTsClient() => clearField(3);

  @$pb.TagNumber(4)
  $fixnum.Int64 get seq => $_getI64(3);
  @$pb.TagNumber(4)
  set seq($fixnum.Int64 v) { $_setInt64(3, v); }
  @$pb.TagNumber(4)
  $core.bool hasSeq() => $_has(3);
  @$pb.TagNumber(4)
  void clearSeq() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get deviceId => $_getSZ(4);
  @$pb.TagNumber(5)
  set deviceId($core.String v) { $_setString(4, v); }
  @$pb.TagNumber(5)
  $core.bool hasDeviceId() => $_has(4);
  @$pb.TagNumber(5)
  void clearDeviceId() => clearField(5);

  @$pb.TagNumber(6)
  $core.String get sessionId => $_getSZ(5);
  @$pb.TagNumber(6)
  set sessionId($core.String v) { $_setString(5, v); }
  @$pb.TagNumber(6)
  $core.bool hasSessionId() => $_has(5);
  @$pb.TagNumber(6)
  void clearSessionId() => clearField(6);

  @$pb.TagNumber(7)
  $core.String get anonymousId => $_getSZ(6);
  @$pb.TagNumber(7)
  set anonymousId($core.String v) { $_setString(6, v); }
  @$pb.TagNumber(7)
  $core.bool hasAnonymousId() => $_has(6);
  @$pb.TagNumber(7)
  void clearAnonymousId() => clearField(7);

  @$pb.TagNumber(8)
  $core.String get userId => $_getSZ(7);
  @$pb.TagNumber(8)
  set userId($core.String v) { $_setString(7, v); }
  @$pb.TagNumber(8)
  $core.bool hasUserId() => $_has(7);
  @$pb.TagNumber(8)
  void clearUserId() => clearField(8);

  @$pb.TagNumber(9)
  $core.Map<$core.String, Value> get props => $_getMap(8);

  @$pb.TagNumber(10)
  Context get context => $_getN(9);
  @$pb.TagNumber(10)
  set context(Context v) { setField(10, v); }
  @$pb.TagNumber(10)
  $core.bool hasContext() => $_has(9);
  @$pb.TagNumber(10)
  void clearContext() => clearField(10);
  @$pb.TagNumber(10)
  Context ensureContext() => $_ensure(9);
}


const _omitFieldNames = $core.bool.fromEnvironment('protobuf.omit_field_names');
const _omitMessageNames = $core.bool.fromEnvironment('protobuf.omit_message_names');
