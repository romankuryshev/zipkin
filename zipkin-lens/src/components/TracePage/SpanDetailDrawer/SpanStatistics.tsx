/*
 * Copyright The OpenZipkin Authors
 * SPDX-License-Identifier: Apache-2.0
 */
import {
  Box,
  Collapse,
  IconButton,
  LinearProgress,
  makeStyles,
  Theme,
  Typography,
} from '@material-ui/core';
import {
  KeyboardArrowDown as KeyboardArrowDownIcon,
  KeyboardArrowUp as KeyboardArrowUpIcon,
} from '@material-ui/icons';
import React from 'react';
import { useToggle } from 'react-use';

const useStyles = makeStyles<Theme>((theme) => ({
  statItem: {
    padding: theme.spacing(1.5),
    backgroundColor: theme.palette.grey[50],
    borderRadius: theme.spacing(0.5),
    border: `1px solid ${theme.palette.divider}`,
  },
  statLabel: {
    fontSize: '0.75rem',
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.5px',
    marginBottom: theme.spacing(0.75),
  },
  statValue: {
    fontSize: '1.25rem',
    fontWeight: 500,
  },
  percentileContainer: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    marginBottom: theme.spacing(0.75),
  },
  percentileLabel: {
    fontSize: '0.875rem',
    color: theme.palette.text.secondary,
    minWidth: '50px',
  },
  percentileValue: {
    fontSize: '0.875rem',
    fontWeight: 500,
    minWidth: '80px',
    textAlign: 'right',
  },
  successBar: {
    backgroundColor: theme.palette.success.light,
    height: 4,
    borderRadius: 2,
  },
  errorBar: {
    backgroundColor: theme.palette.error.light,
    height: 4,
    borderRadius: 2,
    marginTop: theme.spacing(0.5),
  },
  successText: {
    color: theme.palette.success.main,
    fontWeight: 500,
  },
  errorText: {
    color: theme.palette.error.main,
    fontWeight: 500,
  },
  statGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(2, 1fr)',
    gap: theme.spacing(1.5),
    marginTop: theme.spacing(1),
  },
  fullWidthItem: {
    gridColumn: '1 / -1',
  },
}));

type SpanStatisticsProps = {
  spanName: string;
  medianDuration: number | string; // в микросекундах, может быть строкой из JSON
  averageDuration: number | string;
  p50: number | string;
  p95: number | string;
  p99: number | string;
  successCount: number | string;
  errorCount: number | string;
  totalCount: number | string;
};

// Утилита для форматирования времени
const formatDuration = (microseconds: number | string): string => {
  const value =
    typeof microseconds === 'string' ? parseFloat(microseconds) : microseconds;
  if (value < 1000) {
    return `${Math.round(value)} μs`;
  } else if (value < 1000000) {
    return `${(value / 1000).toFixed(2)} ms`;
  } else {
    return `${(value / 1000000).toFixed(2)} s`;
  }
};

export const SpanStatistics = ({
  spanName,
  medianDuration,
  averageDuration,
  p50,
  p95,
  p99,
  successCount,
  errorCount,
  totalCount,
}: SpanStatisticsProps) => {
  const classes = useStyles();
  const [open, toggleOpen] = useToggle(true);

  const totalCountNum =
    typeof totalCount === 'string' ? parseInt(totalCount, 10) : totalCount;
  const successCountNum =
    typeof successCount === 'string'
      ? parseInt(successCount, 10)
      : successCount;
  const errorCountNum =
    typeof errorCount === 'string' ? parseInt(errorCount, 10) : errorCount;

  const successRate =
    totalCountNum > 0 ? (successCountNum / totalCountNum) * 100 : 0;
  const errorRate =
    totalCountNum > 0 ? (errorCountNum / totalCountNum) * 100 : 0;

  return (
    <Box>
      <Box display="flex" justifyContent="space-between" alignItems="center">
        <Typography>Span Statistics ({spanName})</Typography>
        <IconButton onClick={toggleOpen} size="small">
          {open ? <KeyboardArrowUpIcon /> : <KeyboardArrowDownIcon />}
        </IconButton>
      </Box>
      <Collapse in={open}>
        <Box mt={1.5} className={classes.statGrid}>
          {/* Duration Stats */}
          <Box className={classes.statItem}>
            <Typography className={classes.statLabel}>
              Median Duration
            </Typography>
            <Typography className={classes.statValue}>
              {formatDuration(medianDuration)}
            </Typography>
          </Box>

          <Box className={classes.statItem}>
            <Typography className={classes.statLabel}>
              Average Duration
            </Typography>
            <Typography className={classes.statValue}>
              {formatDuration(averageDuration)}
            </Typography>
          </Box>

          {/* Percentiles */}
          <Box className={`${classes.statItem} ${classes.fullWidthItem}`}>
            <Typography className={classes.statLabel}>
              Response Time Percentiles
            </Typography>
            <Box mt={1}>
              <Box className={classes.percentileContainer}>
                <Typography className={classes.percentileLabel}>P50</Typography>
                <Typography className={classes.percentileValue}>
                  {formatDuration(p50)}
                </Typography>
              </Box>
              <Box className={classes.percentileContainer}>
                <Typography className={classes.percentileLabel}>P95</Typography>
                <Typography className={classes.percentileValue}>
                  {formatDuration(p95)}
                </Typography>
              </Box>
              <Box className={classes.percentileContainer}>
                <Typography className={classes.percentileLabel}>P99</Typography>
                <Typography className={classes.percentileValue}>
                  {formatDuration(p99)}
                </Typography>
              </Box>
            </Box>
          </Box>

          {/* Success/Error Rate */}
          <Box className={`${classes.statItem} ${classes.fullWidthItem}`}>
            <Typography className={classes.statLabel}>
              Success / Error Rate
            </Typography>
            <Box mt={1}>
              <Box display="flex" alignItems="center" mb={1}>
                <Typography className={classes.successText}>
                  {successRate.toFixed(1)}%
                </Typography>
                <Box flex={1} mx={1}>
                  <LinearProgress
                    variant="determinate"
                    value={successRate}
                    className={classes.successBar}
                  />
                </Box>
                <Typography variant="caption" color="textSecondary">
                  {successCountNum}
                </Typography>
              </Box>
              <Box display="flex" alignItems="center">
                <Typography className={classes.errorText}>
                  {errorRate.toFixed(1)}%
                </Typography>
                <Box flex={1} mx={1}>
                  <LinearProgress
                    variant="determinate"
                    value={errorRate}
                    className={classes.errorBar}
                  />
                </Box>
                <Typography variant="caption" color="textSecondary">
                  {errorCountNum}
                </Typography>
              </Box>
            </Box>
            <Box mt={1}>
              <Typography variant="caption" color="textSecondary">
                Total: {totalCountNum} spans
              </Typography>
            </Box>
          </Box>
        </Box>
      </Collapse>
    </Box>
  );
};
