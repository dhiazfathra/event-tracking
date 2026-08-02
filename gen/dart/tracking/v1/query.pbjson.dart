//
//  Generated code. Do not modify.
//  source: tracking/v1/query.proto
//
// @dart = 2.12

// ignore_for_file: annotate_overrides, camel_case_types, comment_references
// ignore_for_file: constant_identifier_names, library_prefixes
// ignore_for_file: non_constant_identifier_names, prefer_final_fields
// ignore_for_file: unnecessary_import, unnecessary_this, unused_import

import 'dart:convert' as $convert;
import 'dart:core' as $core;
import 'dart:typed_data' as $typed_data;

@$core.Deprecated('Use opDescriptor instead')
const Op$json = {
  '1': 'Op',
  '2': [
    {'1': 'OP_UNSPECIFIED', '2': 0},
    {'1': 'OP_EQ', '2': 1},
    {'1': 'OP_NEQ', '2': 2},
    {'1': 'OP_IN', '2': 3},
    {'1': 'OP_GT', '2': 4},
    {'1': 'OP_LT', '2': 5},
  ],
};

/// Descriptor for `Op`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List opDescriptor = $convert.base64Decode(
    'CgJPcBISCg5PUF9VTlNQRUNJRklFRBAAEgkKBU9QX0VREAESCgoGT1BfTkVREAISCQoFT1BfSU'
    '4QAxIJCgVPUF9HVBAEEgkKBU9QX0xUEAU=');

@$core.Deprecated('Use intervalDescriptor instead')
const Interval$json = {
  '1': 'Interval',
  '2': [
    {'1': 'INTERVAL_UNSPECIFIED', '2': 0},
    {'1': 'INTERVAL_HOUR', '2': 1},
    {'1': 'INTERVAL_DAY', '2': 2},
    {'1': 'INTERVAL_WEEK', '2': 3},
  ],
};

/// Descriptor for `Interval`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List intervalDescriptor = $convert.base64Decode(
    'CghJbnRlcnZhbBIYChRJTlRFUlZBTF9VTlNQRUNJRklFRBAAEhEKDUlOVEVSVkFMX0hPVVIQAR'
    'IQCgxJTlRFUlZBTF9EQVkQAhIRCg1JTlRFUlZBTF9XRUVLEAM=');

@$core.Deprecated('Use metricDescriptor instead')
const Metric$json = {
  '1': 'Metric',
  '2': [
    {'1': 'METRIC_UNSPECIFIED', '2': 0},
    {'1': 'METRIC_EVENTS', '2': 1},
    {'1': 'METRIC_USERS', '2': 2},
  ],
};

/// Descriptor for `Metric`. Decode as a `google.protobuf.EnumDescriptorProto`.
final $typed_data.Uint8List metricDescriptor = $convert.base64Decode(
    'CgZNZXRyaWMSFgoSTUVUUklDX1VOU1BFQ0lGSUVEEAASEQoNTUVUUklDX0VWRU5UUxABEhAKDE'
    '1FVFJJQ19VU0VSUxAC');

@$core.Deprecated('Use filterDescriptor instead')
const Filter$json = {
  '1': 'Filter',
  '2': [
    {'1': 'field', '3': 1, '4': 1, '5': 9, '10': 'field'},
    {'1': 'op', '3': 2, '4': 1, '5': 14, '6': '.tracking.v1.Op', '10': 'op'},
    {'1': 'values', '3': 3, '4': 3, '5': 9, '10': 'values'},
  ],
};

/// Descriptor for `Filter`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List filterDescriptor = $convert.base64Decode(
    'CgZGaWx0ZXISFAoFZmllbGQYASABKAlSBWZpZWxkEh8KAm9wGAIgASgOMg8udHJhY2tpbmcudj'
    'EuT3BSAm9wEhYKBnZhbHVlcxgDIAMoCVIGdmFsdWVz');

@$core.Deprecated('Use timeseriesRequestDescriptor instead')
const TimeseriesRequest$json = {
  '1': 'TimeseriesRequest',
  '2': [
    {'1': 'event_name', '3': 1, '4': 1, '5': 9, '10': 'eventName'},
    {'1': 'from_ms', '3': 2, '4': 1, '5': 3, '10': 'fromMs'},
    {'1': 'to_ms', '3': 3, '4': 1, '5': 3, '10': 'toMs'},
    {'1': 'interval', '3': 4, '4': 1, '5': 14, '6': '.tracking.v1.Interval', '10': 'interval'},
    {'1': 'metric', '3': 5, '4': 1, '5': 14, '6': '.tracking.v1.Metric', '10': 'metric'},
    {'1': 'filters', '3': 6, '4': 3, '5': 11, '6': '.tracking.v1.Filter', '10': 'filters'},
    {'1': 'group_by', '3': 7, '4': 3, '5': 9, '10': 'groupBy'},
    {'1': 'approximate', '3': 8, '4': 1, '5': 8, '10': 'approximate'},
  ],
};

/// Descriptor for `TimeseriesRequest`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List timeseriesRequestDescriptor = $convert.base64Decode(
    'ChFUaW1lc2VyaWVzUmVxdWVzdBIdCgpldmVudF9uYW1lGAEgASgJUglldmVudE5hbWUSFwoHZn'
    'JvbV9tcxgCIAEoA1IGZnJvbU1zEhMKBXRvX21zGAMgASgDUgR0b01zEjEKCGludGVydmFsGAQg'
    'ASgOMhUudHJhY2tpbmcudjEuSW50ZXJ2YWxSCGludGVydmFsEisKBm1ldHJpYxgFIAEoDjITLn'
    'RyYWNraW5nLnYxLk1ldHJpY1IGbWV0cmljEi0KB2ZpbHRlcnMYBiADKAsyEy50cmFja2luZy52'
    'MS5GaWx0ZXJSB2ZpbHRlcnMSGQoIZ3JvdXBfYnkYByADKAlSB2dyb3VwQnkSIAoLYXBwcm94aW'
    '1hdGUYCCABKAhSC2FwcHJveGltYXRl');

@$core.Deprecated('Use pointDescriptor instead')
const Point$json = {
  '1': 'Point',
  '2': [
    {'1': 'bucket_ms', '3': 1, '4': 1, '5': 3, '10': 'bucketMs'},
    {'1': 'value', '3': 2, '4': 1, '5': 4, '10': 'value'},
  ],
};

/// Descriptor for `Point`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List pointDescriptor = $convert.base64Decode(
    'CgVQb2ludBIbCglidWNrZXRfbXMYASABKANSCGJ1Y2tldE1zEhQKBXZhbHVlGAIgASgEUgV2YW'
    'x1ZQ==');

@$core.Deprecated('Use seriesDescriptor instead')
const Series$json = {
  '1': 'Series',
  '2': [
    {'1': 'group', '3': 1, '4': 3, '5': 11, '6': '.tracking.v1.Series.GroupEntry', '10': 'group'},
    {'1': 'points', '3': 2, '4': 3, '5': 11, '6': '.tracking.v1.Point', '10': 'points'},
  ],
  '3': [Series_GroupEntry$json],
};

@$core.Deprecated('Use seriesDescriptor instead')
const Series_GroupEntry$json = {
  '1': 'GroupEntry',
  '2': [
    {'1': 'key', '3': 1, '4': 1, '5': 9, '10': 'key'},
    {'1': 'value', '3': 2, '4': 1, '5': 9, '10': 'value'},
  ],
  '7': {'7': true},
};

/// Descriptor for `Series`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List seriesDescriptor = $convert.base64Decode(
    'CgZTZXJpZXMSNAoFZ3JvdXAYASADKAsyHi50cmFja2luZy52MS5TZXJpZXMuR3JvdXBFbnRyeV'
    'IFZ3JvdXASKgoGcG9pbnRzGAIgAygLMhIudHJhY2tpbmcudjEuUG9pbnRSBnBvaW50cxo4CgpH'
    'cm91cEVudHJ5EhAKA2tleRgBIAEoCVIDa2V5EhQKBXZhbHVlGAIgASgJUgV2YWx1ZToCOAE=');

@$core.Deprecated('Use timeseriesResponseDescriptor instead')
const TimeseriesResponse$json = {
  '1': 'TimeseriesResponse',
  '2': [
    {'1': 'series', '3': 1, '4': 3, '5': 11, '6': '.tracking.v1.Series', '10': 'series'},
    {'1': 'source', '3': 2, '4': 1, '5': 9, '10': 'source'},
    {'1': 'approximate', '3': 3, '4': 1, '5': 8, '10': 'approximate'},
    {'1': 'computed_at', '3': 4, '4': 1, '5': 3, '10': 'computedAt'},
    {'1': 'etag', '3': 5, '4': 1, '5': 9, '10': 'etag'},
  ],
};

/// Descriptor for `TimeseriesResponse`. Decode as a `google.protobuf.DescriptorProto`.
final $typed_data.Uint8List timeseriesResponseDescriptor = $convert.base64Decode(
    'ChJUaW1lc2VyaWVzUmVzcG9uc2USKwoGc2VyaWVzGAEgAygLMhMudHJhY2tpbmcudjEuU2VyaW'
    'VzUgZzZXJpZXMSFgoGc291cmNlGAIgASgJUgZzb3VyY2USIAoLYXBwcm94aW1hdGUYAyABKAhS'
    'C2FwcHJveGltYXRlEh8KC2NvbXB1dGVkX2F0GAQgASgDUgpjb21wdXRlZEF0EhIKBGV0YWcYBS'
    'ABKAlSBGV0YWc=');

