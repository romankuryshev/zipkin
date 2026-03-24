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
  medianDuration: number; // в микросекундах
  averageDuration: number;
  p50: number;
  p95: number;
  p99: number;
  successCount: number;
  errorCount: number;
  totalCount: number;
};

// Утилита для форматирования времени
const formatDuration = (microseconds: number): string => {
  if (microseconds < 1000) {
    return `${Math.round(microseconds)} μs`;
  } else if (microseconds < 1000000) {
    return `${(microseconds / 1000).toFixed(2)} ms`;
  } else {
    return `${(microseconds / 1000000).toFixed(2)} s`;
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

  const successRate = totalCount > 0 ? (successCount / totalCount) * 100 : 0;
  const errorRate = totalCount > 0 ? (errorCount / totalCount) * 100 : 0;

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
                  {successCount}
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
                  {errorCount}
                </Typography>
              </Box>
            </Box>
            <Box mt={1}>
              <Typography variant="caption" color="textSecondary">
                Total: {totalCount} spans
              </Typography>
            </Box>
          </Box>
        </Box>
      </Collapse>
    </Box>
  );
};
