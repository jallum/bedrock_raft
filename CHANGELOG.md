# Changelog

## [0.9.2] - 2025-08-03

### Added
- Immediate consensus for single-node clusters (quorum = 0)
- When a cluster has no peers, transactions now commit immediately without waiting for followers
- Comprehensive test coverage for single-node consensus scenarios

### Changed
- Optimized transaction processing for single-node deployments
- Single-node clusters now trigger `consensus_reached` callback immediately upon transaction addition