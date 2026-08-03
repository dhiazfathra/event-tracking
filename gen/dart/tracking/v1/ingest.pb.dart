//
//  Generated code. Do not modify.
//  source: tracking/v1/ingest.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:core' as $core;

import 'package:fixnum/fixnum.dart' as $fixnum;
import 'package:protobuf/protobuf.dart' as $pb;

import 'event.pb.dart' as $0;

class BatchRequest extends $pb.GeneratedMessage {
  factory BatchRequest({
    $fixnum.Int64? sentAt,
    $core.Iterable<$0.Event>? events,
  }) {
    final $result = create();
    if (sentAt != null) {
      $result.sentAt = sentAt;
    }
    if (events != null) {
      $result.events.addAll(events);
    }
    return $result;
  }
  BatchRequest._() : super();
  factory BatchRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory BatchRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'BatchRequest', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aInt64(1, _omitFieldNames ? '' : 'sentAt')
    ..pc<$0.Event>(2, _omitFieldNames ? '' : 'events', $pb.PbFieldType.PM, subBuilder: $0.Event.create)
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  BatchRequest clone() => BatchRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  BatchRequest copyWith(void Function(BatchRequest) updates) => super.copyWith((message) => updates(message as BatchRequest)) as BatchRequest;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static BatchRequest create() => BatchRequest._();
  BatchRequest createEmptyInstance() => create();
  static $pb.PbList<BatchRequest> createRepeated() => $pb.PbList<BatchRequest>();
  @$core.pragma('dart2js:noInline')
  static BatchRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<BatchRequest>(create);
  static BatchRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get sentAt => $_getI64(0);
  @$pb.TagNumber(1)
  set sentAt($fixnum.Int64 v) { $_setInt64(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasSentAt() => $_has(0);
  @$pb.TagNumber(1)
  void clearSentAt() => clearField(1);

  @$pb.TagNumber(2)
  $core.List<$0.Event> get events => $_getList(1);
}

/// Reject codes. Stable strings — the SDK logs them and support reads them.
class Reject extends $pb.GeneratedMessage {
  factory Reject({
    $core.String? eventId,
    $core.String? code,
    $core.String? message,
  }) {
    final $result = create();
    if (eventId != null) {
      $result.eventId = eventId;
    }
    if (code != null) {
      $result.code = code;
    }
    if (message != null) {
      $result.message = message;
    }
    return $result;
  }
  Reject._() : super();
  factory Reject.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory Reject.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'Reject', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'eventId')
    ..aOS(2, _omitFieldNames ? '' : 'code')
    ..aOS(3, _omitFieldNames ? '' : 'message')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  Reject clone() => Reject()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  Reject copyWith(void Function(Reject) updates) => super.copyWith((message) => updates(message as Reject)) as Reject;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static Reject create() => Reject._();
  Reject createEmptyInstance() => create();
  static $pb.PbList<Reject> createRepeated() => $pb.PbList<Reject>();
  @$core.pragma('dart2js:noInline')
  static Reject getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<Reject>(create);
  static Reject? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get eventId => $_getSZ(0);
  @$pb.TagNumber(1)
  set eventId($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasEventId() => $_has(0);
  @$pb.TagNumber(1)
  void clearEventId() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get code => $_getSZ(1);
  @$pb.TagNumber(2)
  set code($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasCode() => $_has(1);
  @$pb.TagNumber(2)
  void clearCode() => clearField(2);

  @$pb.TagNumber(3)
  $core.String get message => $_getSZ(2);
  @$pb.TagNumber(3)
  set message($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasMessage() => $_has(2);
  @$pb.TagNumber(3)
  void clearMessage() => clearField(3);
}

/// Always partial-success shaped. A single malformed event must never fail the
/// whole batch: the client would retry forever and the outbox would never drain.
class BatchResponse extends $pb.GeneratedMessage {
  factory BatchResponse({
    $fixnum.Int64? receivedAt,
    $core.Iterable<$core.String>? accepted,
    $core.Iterable<Reject>? rejected,
  }) {
    final $result = create();
    if (receivedAt != null) {
      $result.receivedAt = receivedAt;
    }
    if (accepted != null) {
      $result.accepted.addAll(accepted);
    }
    if (rejected != null) {
      $result.rejected.addAll(rejected);
    }
    return $result;
  }
  BatchResponse._() : super();
  factory BatchResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory BatchResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'BatchResponse', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aInt64(1, _omitFieldNames ? '' : 'receivedAt')
    ..pPS(2, _omitFieldNames ? '' : 'accepted')
    ..pc<Reject>(3, _omitFieldNames ? '' : 'rejected', $pb.PbFieldType.PM, subBuilder: Reject.create)
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  BatchResponse clone() => BatchResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  BatchResponse copyWith(void Function(BatchResponse) updates) => super.copyWith((message) => updates(message as BatchResponse)) as BatchResponse;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static BatchResponse create() => BatchResponse._();
  BatchResponse createEmptyInstance() => create();
  static $pb.PbList<BatchResponse> createRepeated() => $pb.PbList<BatchResponse>();
  @$core.pragma('dart2js:noInline')
  static BatchResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<BatchResponse>(create);
  static BatchResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $fixnum.Int64 get receivedAt => $_getI64(0);
  @$pb.TagNumber(1)
  set receivedAt($fixnum.Int64 v) { $_setInt64(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasReceivedAt() => $_has(0);
  @$pb.TagNumber(1)
  void clearReceivedAt() => clearField(1);

  @$pb.TagNumber(2)
  $core.List<$core.String> get accepted => $_getList(1);

  @$pb.TagNumber(3)
  $core.List<Reject> get rejected => $_getList(2);
}

class TokenRequest extends $pb.GeneratedMessage {
  factory TokenRequest({
    $core.String? clientId,
    $core.String? platform,
    $core.String? attestation,
    $core.String? challenge,
    $core.String? deviceHint,
  }) {
    final $result = create();
    if (clientId != null) {
      $result.clientId = clientId;
    }
    if (platform != null) {
      $result.platform = platform;
    }
    if (attestation != null) {
      $result.attestation = attestation;
    }
    if (challenge != null) {
      $result.challenge = challenge;
    }
    if (deviceHint != null) {
      $result.deviceHint = deviceHint;
    }
    return $result;
  }
  TokenRequest._() : super();
  factory TokenRequest.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory TokenRequest.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'TokenRequest', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'clientId')
    ..aOS(2, _omitFieldNames ? '' : 'platform')
    ..aOS(3, _omitFieldNames ? '' : 'attestation')
    ..aOS(4, _omitFieldNames ? '' : 'challenge')
    ..aOS(5, _omitFieldNames ? '' : 'deviceHint')
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  TokenRequest clone() => TokenRequest()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  TokenRequest copyWith(void Function(TokenRequest) updates) => super.copyWith((message) => updates(message as TokenRequest)) as TokenRequest;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TokenRequest create() => TokenRequest._();
  TokenRequest createEmptyInstance() => create();
  static $pb.PbList<TokenRequest> createRepeated() => $pb.PbList<TokenRequest>();
  @$core.pragma('dart2js:noInline')
  static TokenRequest getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TokenRequest>(create);
  static TokenRequest? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get clientId => $_getSZ(0);
  @$pb.TagNumber(1)
  set clientId($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasClientId() => $_has(0);
  @$pb.TagNumber(1)
  void clearClientId() => clearField(1);

  @$pb.TagNumber(2)
  $core.String get platform => $_getSZ(1);
  @$pb.TagNumber(2)
  set platform($core.String v) { $_setString(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasPlatform() => $_has(1);
  @$pb.TagNumber(2)
  void clearPlatform() => clearField(2);

  @$pb.TagNumber(3)
  $core.String get attestation => $_getSZ(2);
  @$pb.TagNumber(3)
  set attestation($core.String v) { $_setString(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasAttestation() => $_has(2);
  @$pb.TagNumber(3)
  void clearAttestation() => clearField(3);

  @$pb.TagNumber(4)
  $core.String get challenge => $_getSZ(3);
  @$pb.TagNumber(4)
  set challenge($core.String v) { $_setString(3, v); }
  @$pb.TagNumber(4)
  $core.bool hasChallenge() => $_has(3);
  @$pb.TagNumber(4)
  void clearChallenge() => clearField(4);

  @$pb.TagNumber(5)
  $core.String get deviceHint => $_getSZ(4);
  @$pb.TagNumber(5)
  set deviceHint($core.String v) { $_setString(4, v); }
  @$pb.TagNumber(5)
  $core.bool hasDeviceHint() => $_has(4);
  @$pb.TagNumber(5)
  void clearDeviceHint() => clearField(5);
}

/// trust_tier on the wire matches the JWT claim and the ClickHouse column:
/// 0 = attested, 1 = attestation unavailable. Not an enum — this is a plain
/// tier number that has to stay bit-for-bit consistent with the uint8 claim
/// pkg/tenant mints and pkg/clickhouse stores, and juggling an enum with
/// different numbering across three layers is how they drift.
class TokenResponse extends $pb.GeneratedMessage {
  factory TokenResponse({
    $core.String? accessToken,
    $fixnum.Int64? expiresIn,
    $core.int? trustTier,
  }) {
    final $result = create();
    if (accessToken != null) {
      $result.accessToken = accessToken;
    }
    if (expiresIn != null) {
      $result.expiresIn = expiresIn;
    }
    if (trustTier != null) {
      $result.trustTier = trustTier;
    }
    return $result;
  }
  TokenResponse._() : super();
  factory TokenResponse.fromBuffer($core.List<$core.int> i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromBuffer(i, r);
  factory TokenResponse.fromJson($core.String i, [$pb.ExtensionRegistry r = $pb.ExtensionRegistry.EMPTY]) => create()..mergeFromJson(i, r);

  static final $pb.BuilderInfo _i = $pb.BuilderInfo(_omitMessageNames ? '' : 'TokenResponse', package: const $pb.PackageName(_omitMessageNames ? '' : 'tracking.v1'), createEmptyInstance: create)
    ..aOS(1, _omitFieldNames ? '' : 'accessToken')
    ..aInt64(2, _omitFieldNames ? '' : 'expiresIn')
    ..a<$core.int>(3, _omitFieldNames ? '' : 'trustTier', $pb.PbFieldType.OU3)
    ..hasRequiredFields = false
  ;

  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.deepCopy] instead. '
  'Will be removed in next major version')
  TokenResponse clone() => TokenResponse()..mergeFromMessage(this);
  @$core.Deprecated(
  'Using this can add significant overhead to your binary. '
  'Use [GeneratedMessageGenericExtensions.rebuild] instead. '
  'Will be removed in next major version')
  TokenResponse copyWith(void Function(TokenResponse) updates) => super.copyWith((message) => updates(message as TokenResponse)) as TokenResponse;

  $pb.BuilderInfo get info_ => _i;

  @$core.pragma('dart2js:noInline')
  static TokenResponse create() => TokenResponse._();
  TokenResponse createEmptyInstance() => create();
  static $pb.PbList<TokenResponse> createRepeated() => $pb.PbList<TokenResponse>();
  @$core.pragma('dart2js:noInline')
  static TokenResponse getDefault() => _defaultInstance ??= $pb.GeneratedMessage.$_defaultFor<TokenResponse>(create);
  static TokenResponse? _defaultInstance;

  @$pb.TagNumber(1)
  $core.String get accessToken => $_getSZ(0);
  @$pb.TagNumber(1)
  set accessToken($core.String v) { $_setString(0, v); }
  @$pb.TagNumber(1)
  $core.bool hasAccessToken() => $_has(0);
  @$pb.TagNumber(1)
  void clearAccessToken() => clearField(1);

  @$pb.TagNumber(2)
  $fixnum.Int64 get expiresIn => $_getI64(1);
  @$pb.TagNumber(2)
  set expiresIn($fixnum.Int64 v) { $_setInt64(1, v); }
  @$pb.TagNumber(2)
  $core.bool hasExpiresIn() => $_has(1);
  @$pb.TagNumber(2)
  void clearExpiresIn() => clearField(2);

  @$pb.TagNumber(3)
  $core.int get trustTier => $_getIZ(2);
  @$pb.TagNumber(3)
  set trustTier($core.int v) { $_setUnsignedInt32(2, v); }
  @$pb.TagNumber(3)
  $core.bool hasTrustTier() => $_has(2);
  @$pb.TagNumber(3)
  void clearTrustTier() => clearField(3);
}


const _omitFieldNames = $core.bool.fromEnvironment('protobuf.omit_field_names');
const _omitMessageNames = $core.bool.fromEnvironment('protobuf.omit_message_names');
