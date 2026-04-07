/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
import { ServiceNameAndSpanCount } from './TraceSummary';

export type AdjustedAnnotation = {
  value: string;
  timestamp: number;
  endpoint: string; // Ex. 'fooo' or 'unknown' on null span.localEndpoint
  relativeTime?: string;
};

export type AdjustedSpan = {
  spanId: string;
  spanName: string; // span.name or 'unknown' on null
  serviceName: string; // span.localEndpoint.serviceName or 'unknown' on null
  kind?: 'CLIENT' | 'SERVER' | 'PRODUCER' | 'CONSUMER';
  parentId?: string;
  childIds: string[];
  serviceNames: string[];
  timestamp: number;
  duration: number;
  durationStr: string;
  tags: {
    key: string;
    value: string;
  }[];
  annotations: AdjustedAnnotation[];
  errorType: string;
  depth: number;
  width: number;
  left: number;
  statistics?: {
    spanName: string;
    spanKind: string;
    medianDuration: number | string;
    averageDuration: number | string;
    p50: number | string;
    p95: number | string;
    p99: number | string;
    successCount: number | string;
    errorCount: number | string;
    totalCount: number | string;
  };
};

type AdjustedTrace = {
  traceId: string;
  serviceNameAndSpanCounts: ServiceNameAndSpanCount[];
  duration: number;
  durationStr: string;
  // the root-most span, when the root is missing
  rootSpan: {
    serviceName: string; // span.localEndpoint.serviceName or 'unknown' on null
    spanName: string; // span.name or 'unknown' on null
  };
  spans: AdjustedSpan[];
};

export default AdjustedTrace;
