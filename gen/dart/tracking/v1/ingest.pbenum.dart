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

import 'package:protobuf/protobuf.dart' as $pb;

/// TrustTier is deliberately non-zero for real states: proto3 decodes an
/// omitted field as 0, so if 0 meant "attested" a missing trust_tier would be
/// indistinguishable from an attested response.
class TrustTier extends $pb.ProtobufEnum {
  static const TrustTier TRUST_TIER_UNSPECIFIED = TrustTier._(0, _omitEnumNames ? '' : 'TRUST_TIER_UNSPECIFIED');
  static const TrustTier TRUST_TIER_ATTESTED = TrustTier._(1, _omitEnumNames ? '' : 'TRUST_TIER_ATTESTED');
  static const TrustTier TRUST_TIER_ATTESTATION_UNAVAILABLE = TrustTier._(2, _omitEnumNames ? '' : 'TRUST_TIER_ATTESTATION_UNAVAILABLE');

  static const $core.List<TrustTier> values = <TrustTier> [
    TRUST_TIER_UNSPECIFIED,
    TRUST_TIER_ATTESTED,
    TRUST_TIER_ATTESTATION_UNAVAILABLE,
  ];

  static final $core.Map<$core.int, TrustTier> _byValue = $pb.ProtobufEnum.initByValue(values);
  static TrustTier? valueOf($core.int value) => _byValue[value];

  const TrustTier._($core.int v, $core.String n) : super(v, n);
}


const _omitEnumNames = $core.bool.fromEnvironment('protobuf.omit_enum_names');
