# Notes

## Timeline

| Phase | Duration | Notes |
| ------ | ------ | ----- |
| Download all EPSS scores | ~1 minute | ~104,781,051 data points |
| Reduce EPSS scores to unique values | ~1m | ~753,395 data points, 99.2% reduction |
| Partition EPSS scores by date | ~1.5s | 525 partitions |
| Partition EPSS scores by CVE ID | ~3m | 221,242 partitions |

## Optimizations

### Dropping duplicate data

Before:

- Total rows: 104781051
- Total size: 15.7 GB (in-memory)

After:

- Total rows: 753395
- Total size: 62.12 MB (in-memory)

> Reduction: 99.2%
