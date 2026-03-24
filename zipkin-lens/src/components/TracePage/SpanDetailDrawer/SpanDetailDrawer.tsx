/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
import { Box, Divider, Grid, makeStyles, Typography } from '@material-ui/core';
import React from 'react';
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
  // Данные для статистики (опционально)
  spanStats?: {
    medianDuration: number;
    averageDuration: number;
    p50: number;
    p95: number;
    p99: number;
    successCount: number;
    errorCount: number;
    totalCount: number;
  };
};

export const SpanDetailDrawer = ({
  span,
  minTimestamp,
  spanStats,
}: SpanDetailDrawerProps) => {
  const classes = useStyles();
  const { t } = useTranslation();

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
      {spanStats && (
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
