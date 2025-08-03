# Changelog

## [0.9.5] - 2025-08-03

### Changed
- Single-node clusters now start as followers and go through election process like multi-node clusters
- Candidate mode checks for immediate leadership eligibility in single-node scenarios

### Fixed
- Single-node clusters now increment terms during elections instead of remaining at term 0

## [0.9.4] - 2025-08-03

### Fixed
- **Critical Raft specification compliance**: Fixed `currentTerm` persistence across server restarts
- Added proper term storage to Log protocol with `current_term/1` and `save_current_term/2` methods
- Terms are now persisted to stable storage before responding to RPCs as required by Raft specification
- Fixed term initialization to restore from persistent storage instead of deriving from transaction IDs

### Changed
- Enhanced TupleInMemoryLog and BinaryInMemoryLog to include independent term storage
- Updated Raft initialization to comply with "persistent state" requirements from Raft paper

## [0.9.3] - 2025-08-03

### Fixed
- Leader now properly initializes `id_sequence` from existing log to prevent transaction ID conflicts
- Fixed `consensus_reached` callbacks to pass committed log instance instead of stale log reference
- Prevents leaders from attempting to create transactions with IDs that already exist in recovered logs

## [0.9.2] - 2025-08-03

### Added
- Immediate consensus for single-node clusters (quorum = 0)
- When a cluster has no peers, transactions now commit immediately without waiting for followers
- Comprehensive test coverage for single-node consensus scenarios

### Changed
- Optimized transaction processing for single-node deployments
- Single-node clusters now trigger `consensus_reached` callback immediately upon transaction addition