//
//  Generated code. Do not modify.
//  source: tracking/v1/ingest.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use batchRequestDescriptor instead')
const BatchRequest$json = {
  '1': 'BatchRequest',
  '2': [
    {'1': 'sent_at', '3': 1, '4': 1, '5': 3, '10': 'sentAt'},
    {'1': 'events', '3': 2, '4': 3, '5': 11, '6': '.tracking.v1.Event', '10': 'events'},
  ],
};

/// Descriptor for `BatchRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List batchRequestDescriptor = $convert.base64Decode(
    'CgxCYXRjaFJlcXVlc3QSFwoHc2VudF9hdBgBIAEoA1IGc2VudEF0EioKBmV2ZW50cxgCIAMoCz'
    'ISLnRyYWNraW5nLnYxLkV2ZW50UgZldmVudHM=');

@$core.Deprecated('Use rejectDescriptor instead')
const Reject$json = {
  '1': 'Reject',
  '2': [
    {'1': 'event_id', '3': 1, '4': 1, '5': 9, '10': 'eventId'},
    {'1': 'code', '3': 2, '4': 1, '5': 9, '10': 'code'},
    {'1': 'message', '3': 3, '4': 1, '5': 9, '10': 'message'},
  ],
};

/// Descriptor for `Reject`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List rejectDescriptor = $convert.base64Decode(
    'CgZSZWplY3QSGQoIZXZlbnRfaWQYASABKAlSB2V2ZW50SWQSEgoEY29kZRgCIAEoCVIEY29kZR'
    'IYCgdtZXNzYWdlGAMgASgJUgdtZXNzYWdl');

@$core.Deprecated('Use batchResponseDescriptor instead')
const BatchResponse$json = {
  '1': 'BatchResponse',
  '2': [
    {'1': 'received_at', '3': 1, '4': 1, '5': 3, '10': 'receivedAt'},
    {'1': 'accepted', '3': 2, '4': 3, '5': 9, '10': 'accepted'},
    {'1': 'rejected', '3': 3, '4': 3, '5': 11, '6': '.tracking.v1.Reject', '10': 'rejected'},
  ],
};

/// Descriptor for `BatchResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List batchResponseDescriptor = $convert.base64Decode(
    'Cg1CYXRjaFJlc3BvbnNlEh8KC3JlY2VpdmVkX2F0GAEgASgDUgpyZWNlaXZlZEF0EhoKCGFjY2'
    'VwdGVkGAIgAygJUghhY2NlcHRlZBIvCghyZWplY3RlZBgDIAMoCzITLnRyYWNraW5nLnYxLlJl'
    'amVjdFIIcmVqZWN0ZWQ=');

@$core.Deprecated('Use tokenRequestDescriptor instead')
const TokenRequest$json = {
  '1': 'TokenRequest',
  '2': [
    {'1': 'client_id', '3': 1, '4': 1, '5': 9, '10': 'clientId'},
    {'1': 'platform', '3': 2, '4': 1, '5': 9, '10': 'platform'},
    {'1': 'attestation', '3': 3, '4': 1, '5': 9, '10': 'attestation'},
    {'1': 'challenge', '3': 4, '4': 1, '5': 9, '10': 'challenge'},
    {'1': 'device_hint', '3': 5, '4': 1, '5': 9, '10': 'deviceHint'},
  ],
};

/// Descriptor for `TokenRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tokenRequestDescriptor = $convert.base64Decode(
    'CgxUb2tlblJlcXVlc3QSGwoJY2xpZW50X2lkGAEgASgJUghjbGllbnRJZBIaCghwbGF0Zm9ybR'
    'gCIAEoCVIIcGxhdGZvcm0SIAoLYXR0ZXN0YXRpb24YAyABKAlSC2F0dGVzdGF0aW9uEhwKCWNo'
    'YWxsZW5nZRgEIAEoCVIJY2hhbGxlbmdlEh8KC2RldmljZV9oaW50GAUgASgJUgpkZXZpY2VIaW'
    '50');

@$core.Deprecated('Use tokenResponseDescriptor instead')
const TokenResponse$json = {
  '1': 'TokenResponse',
  '2': [
    {'1': 'access_token', '3': 1, '4': 1, '5': 9, '10': 'accessToken'},
    {'1': 'expires_in', '3': 2, '4': 1, '5': 3, '10': 'expiresIn'},
    {'1': 'trust_tier', '3': 3, '4': 1, '5': 13, '10': 'trustTier'},
  ],
};

/// Descriptor for `TokenResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List tokenResponseDescriptor = $convert.base64Decode(
    'Cg1Ub2tlblJlc3BvbnNlEiEKDGFjY2Vzc190b2tlbhgBIAEoCVILYWNjZXNzVG9rZW4SHQoKZX'
    'hwaXJlc19pbhgCIAEoA1IJZXhwaXJlc0luEh0KCnRydXN0X3RpZXIYAyABKA1SCXRydXN0VGll'
    'cg==');

