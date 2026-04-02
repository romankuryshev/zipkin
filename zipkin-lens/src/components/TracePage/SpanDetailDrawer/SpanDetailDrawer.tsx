/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
import { Box, Divider, Grid, makeStyles, Typography } from '@material-ui/core';
import React, { useEffect, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AdjustedSpan } from '../../../models/AdjustedTrace';
import { AnnotationViewer } from './AnnotationViewer';
import { SpanStatistics } from './SpanStatistics';
import { TagList } from './TagList';

const useStyles = makeStyles((theme) => ({
  root: {
    padding: theme.spacing(2),
    backgroundColor: theme.palette.background.paper,
    borderLeft: `1px solid ${theme.palette.divider}`,
    minHeight: '100%',
  },
  basicInfoLabel: {
    lineHeight: 1.2,
  },
  basicInfoValue: {
    wordWrap: 'break-word',
  },
  divider: {
    marginTop: theme.spacing(1.5),
    marginBottom: theme.spacing(2.5),
  },
}));

type SpanDetailDrawerProps = {
  span: AdjustedSpan;
  minTimestamp: number;
};

export const SpanDetailDrawer = ({
  span,
  minTimestamp,
}: SpanDetailDrawerProps) => {
  const classes = useStyles();
  const { t } = useTranslation();
  const [spanStats, setSpanStats] = useState<{
    medianDuration: number;
    averageDuration: number;
    p50: number;
    p95: number;
    p99: number;
    successCount: number;
    errorCount: number;
    totalCount: number;
  } | null>(null);
  const [loading, setLoading] = useState(false);

  // Загружать статистику при открытии drawer'а или при изменении спана
  useEffect(() => {
    if (span) {
      setLoading(true);
      const params = new URLSearchParams({
        serviceName: span.serviceName,
        spanName: span.spanName,
        ...(span.kind && { spanKind: span.kind }),
      });

      console.log('[SpanDetailDrawer] Loading statistics with params:', {
        serviceName: span.serviceName,
        spanName: span.spanName,
        spanKind: span.kind,
        url: `/api/v2/span-statistics?${params}`,
      });

      fetch(`/api/v2/span-statistics?${params}`)
        .then((res) => {
          console.log('[SpanDetailDrawer] Response status:', res.status);
          if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
          }
          return res.json();
        })
        .then((data) => {
          console.log('[SpanDetailDrawer] Loaded statistics:', data);
          const stats = {
            medianDuration: data.medianDuration ?? 0,
            averageDuration: data.averageDuration ?? 0,
            p50: data.p50 ?? 0,
            p95: data.p95 ?? 0,
            p99: data.p99 ?? 0,
            successCount: data.successCount ?? 0,
            errorCount: data.errorCount ?? 0,
            totalCount: data.totalCount ?? 0,
          };
          setSpanStats(stats);
        })
        .catch((err) => {
          console.error(
            '[SpanDetailDrawer] Failed to load span statistics:',
            err,
          );
          // Set default statistics on error
          setSpanStats({
            medianDuration: 0,
            averageDuration: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            successCount: 0,
            errorCount: 0,
            totalCount: 0,
          });
        })
        .finally(() => {
          setLoading(false);
        });
    }
  }, [span.serviceName, span.spanName, span.kind]);

  return (
    <Box className={classes.root}>
      <Grid container spacing={1}>
        {[
          { label: 'Service name', value: span.serviceName },
          { label: 'Span name', value: span.spanName },
          { label: t(`Span ID`), value: span.spanId },
          { label: t(`Parent ID`), value: span.parentId || 'none' },
        ].map(({ label, value }) => (
          <Grid key={label} item xs={6}>
            <Typography
              variant="caption"
              color="textSecondary"
              className={classes.basicInfoLabel}
            >
              {label}
            </Typography>
            <Typography variant="body1" className={classes.basicInfoValue}>
              {value}
            </Typography>
          </Grid>
        ))}
      </Grid>

      {/* Span Statistics Section */}
      {loading && (
        <>
          <Divider className={classes.divider} />
          <Typography variant="caption" color="textSecondary">
            Loading span statistics...
          </Typography>
        </>
      )}
      {!loading && spanStats && (
        <>
          <Divider className={classes.divider} />
          <SpanStatistics
            spanName={span.spanName}
            medianDuration={spanStats.medianDuration}
            averageDuration={spanStats.averageDuration}
            p50={spanStats.p50}
            p95={spanStats.p95}
            p99={spanStats.p99}
            successCount={spanStats.successCount}
            errorCount={spanStats.errorCount}
            totalCount={spanStats.totalCount}
          />
        </>
      )}

      {span.annotations.length > 0 && (
        <>
          <Divider className={classes.divider} />
          <AnnotationViewer minTimestamp={minTimestamp} span={span} />
        </>
      )}
      {span.tags.length > 0 && (
        <>
          <Divider className={classes.divider} />
          <TagList span={span} />
        </>
      )}
    </Box>
  );
};
