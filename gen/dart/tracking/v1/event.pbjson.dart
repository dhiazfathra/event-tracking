//
//  Generated code. Do not modify.
//  source: tracking/v1/event.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use valueDescriptor instead')
const Value$json = {
  '1': 'Value',
  '2': [
    {'1': 'string_value', '3': 1, '4': 1, '5': 9, '9': 0, '10': 'stringValue'},
    {'1': 'number_value', '3': 2, '4': 1, '5': 1, '9': 0, '10': 'numberValue'},
    {'1': 'bool_value', '3': 3, '4': 1, '5': 8, '9': 0, '10': 'boolValue'},
  ],
  '8': [
    {'1': 'kind'},
  ],
};

/// Descriptor for `Value`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List valueDescriptor = $convert.base64Decode(
    'CgVWYWx1ZRIjCgxzdHJpbmdfdmFsdWUYASABKAlIAFILc3RyaW5nVmFsdWUSIwoMbnVtYmVyX3'
    'ZhbHVlGAIgASgBSABSC251bWJlclZhbHVlEh8KCmJvb2xfdmFsdWUYAyABKAhIAFIJYm9vbFZh'
    'bHVlQgYKBGtpbmQ=');

@$core.Deprecated('Use contextDescriptor instead')
const Context$json = {
  '1': 'Context',
  '2': [
    {'1': 'sdk_version', '3': 1, '4': 1, '5': 9, '10': 'sdkVersion'},
    {'1': 'app_version', '3': 2, '4': 1, '5': 9, '10': 'appVersion'},
    {'1': 'os', '3': 3, '4': 1, '5': 9, '10': 'os'},
    {'1': 'os_version', '3': 4, '4': 1, '5': 9, '10': 'osVersion'},
    {'1': 'locale', '3': 5, '4': 1, '5': 9, '10': 'locale'},
  ],
};

/// Descriptor for `Context`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List contextDescriptor = $convert.base64Decode(
    'CgdDb250ZXh0Eh8KC3Nka192ZXJzaW9uGAEgASgJUgpzZGtWZXJzaW9uEh8KC2FwcF92ZXJzaW'
    '9uGAIgASgJUgphcHBWZXJzaW9uEg4KAm9zGAMgASgJUgJvcxIdCgpvc192ZXJzaW9uGAQgASgJ'
    'Uglvc1ZlcnNpb24SFgoGbG9jYWxlGAUgASgJUgZsb2NhbGU=');

@$core.Deprecated('Use eventDescriptor instead')
const Event$json = {
  '1': 'Event',
  '2': [
    {'1': 'event_id', '3': 1, '4': 1, '5': 9, '10': 'eventId'},
    {'1': 'name', '3': 2, '4': 1, '5': 9, '10': 'name'},
    {'1': 'ts_client', '3': 3, '4': 1, '5': 3, '10': 'tsClient'},
    {'1': 'seq', '3': 4, '4': 1, '5': 4, '10': 'seq'},
    {'1': 'device_id', '3': 5, '4': 1, '5': 9, '10': 'deviceId'},
    {'1': 'session_id', '3': 6, '4': 1, '5': 9, '10': 'sessionId'},
    {'1': 'anonymous_id', '3': 7, '4': 1, '5': 9, '10': 'anonymousId'},
    {'1': 'user_id', '3': 8, '4': 1, '5': 9, '10': 'userId'},
    {'1': 'props', '3': 9, '4': 3, '5': 11, '6': '.tracking.v1.Event.PropsEntry', '10': 'props'},
    {'1': 'context', '3': 10, '4': 1, '5': 11, '6': '.tracking.v1.Context', '10': 'context'},
  ],
  '3': [Event_PropsEntry$json],
};

@$core.Deprecated('Use eventDescriptor instead')
const Event_PropsEntry$json = {
  '1': 'PropsEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 11, '6': '.tracking.v1.Value', '10': 'value'},
  ],
  '7': {'7': true},
};

/// Descriptor for `Event`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List eventDescriptor = $convert.base64Decode(
    'CgVFdmVudBIZCghldmVudF9pZBgBIAEoCVIHZXZlbnRJZBISCgRuYW1lGAIgASgJUgRuYW1lEh'
    'sKCXRzX2NsaWVudBgDIAEoA1IIdHNDbGllbnQSEAoDc2VxGAQgASgEUgNzZXESGwoJZGV2aWNl'
    'X2lkGAUgASgJUghkZXZpY2VJZBIdCgpzZXNzaW9uX2lkGAYgASgJUglzZXNzaW9uSWQSIQoMYW'
    '5vbnltb3VzX2lkGAcgASgJUgthbm9ueW1vdXNJZBIXCgd1c2VyX2lkGAggASgJUgZ1c2VySWQS'
    'MwoFcHJvcHMYCSADKAsyHS50cmFja2luZy52MS5FdmVudC5Qcm9wc0VudHJ5UgVwcm9wcxIuCg'
    'djb250ZXh0GAogASgLMhQudHJhY2tpbmcudjEuQ29udGV4dFIHY29udGV4dBpMCgpQcm9wc0Vu'
    'dHJ5EhAKA2tleRgBIAEoCVIDa2V5EigKBXZhbHVlGAIgASgLMhIudHJhY2tpbmcudjEuVmFsdW'
    'VSBXZhbHVlOgI4AQ==');

