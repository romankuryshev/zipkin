/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
import Annotation from './Annotation';
import Endpoint from './Endpoint';

// Refer to https://github.com/openzipkin/zipkin-js/blob/master/packages/zipkin/src/model.js

export type SpanStatistics = {
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

// Same type as Span in the OpenApi/Swagger model https://zipkin.io/zipkin-api/#
type Span = {
  id: string;
  traceId: string;
  name?: string;
  parentId?: string;
  kind?: 'CLIENT' | 'SERVER' | 'PRODUCER' | 'CONSUMER';
  timestamp?: number;
  duration?: number;
  debug?: boolean;
  shared?: boolean;
  localEndpoint?: Endpoint;
  remoteEndpoint?: Endpoint;
  annotations?: Annotation[];
  tags?: { [key: string]: string };
  statistics?: SpanStatistics;
};

export default Span;
