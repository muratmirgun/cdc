# Implementation Plan: CDC Pipeline

## Overview

This implementation plan breaks down the CDC pipeline into discrete, incremental coding tasks. The plan follows a bottom-up approach: building core modules first, then integrating them into complete services, and finally adding testing and observability. Each task builds on previous work, ensuring no orphaned code.

## Tasks

- [ ] 1. Set up project structure and shared modules
  - Create Go module structure for both services
  - Set up shared types and interfaces package
  - Configure dependency management (go.mod)
  - Set up directory structure following the design
  - _Requirements: 13.1, 13.2_

- [ ] 2. Implement shared logging module
  - [ ] 2.1 Create logger package with uber-go/zap
    - Initialize structured logger with configurable levels
    - Implement log formatting (JSON and console)
    - Add contextual field helpers
    - _Requirements: 10.1, 10.2, 10.3, 10.4_
  
  - [ ]* 2.2 Write unit tests for logger
    - Test log level filtering
    - Test structured field inclusion
    - Test error logging with stack traces
    - _Requirements: 10.3, 10.5_

- [ ] 3. Implement shared configuration module
  - [ ] 3.1 Create config package with viper
    - Implement configuration loading from environment variables
    - Implement configuration loading from YAML files
    - Add configuration validation logic
    - Define configuration structs for both services
    - _Requirements: 11.1, 11.2, 11.4, 11.5_
  
  - [ ]* 3.2 Write property test for configuration validation
    - **Property 23: Configuration Validation Rejection**
    - **Validates: Requirements 11.5**
  
  - [ ]* 3.3 Write unit tests for configuration loading
    - Test environment variable loading
    - Test file loading
    - Test missing required configuration
    - _Requirements: 11.1, 11.2, 11.4_

- [ ] 4. Implement retry and backoff utilities
  - [ ] 4.1 Create retry package with exponential backoff
    - Implement exponential backoff calculator
    - Implement retry wrapper with max attempts
    - Add error classification (retryable vs non-retryable)
    - _Requirements: 12.1, 12.2, 12.5_
  
  - [ ]* 4.2 Write property test for exponential backoff
    - **Property 24: Exponential Backoff Retry Pattern**
    - **Validates: Requirements 1.4, 3.4, 12.1, 12.4**
  
  - [ ]* 4.3 Write property test for maximum retry count
    - **Property 25: Maximum Retry Count Enforcement**
    - **Validates: Requirements 12.2**
  
  - [ ]* 4.4 Write property test for error classification
    - **Property 26: Error Type Classification**
    - **Validates: Requirements 12.5**

- [ ] 5. Implement Watcher: Resume token storage
  - [ ] 5.1 Create token storage interface and implementations
    - Define TokenStore interface
    - Implement Redis-based token storage
    - Implement file-based token storage
    - Add connection retry logic for Redis
    - _Requirements: 2.1, 2.2, 2.5_
  
  - [ ]* 5.2 Write property test for token persistence round-trip
    - **Property 3: Resume Token Persistence Round-Trip**
    - **Validates: Requirements 2.2**
  
  - [ ]* 5.3 Write property test for token storage backend equivalence
    - **Property 6: Resume Token Storage Backend Equivalence**
    - **Validates: Requirements 2.5**
  
  - [ ]* 5.4 Write unit tests for token storage
    - Test Redis storage with mock client
    - Test file storage with temp directory
    - Test error handling for storage failures
    - _Requirements: 2.1, 2.4, 2.5_

- [ ] 6. Implement Watcher: Kafka producer
  - [ ] 6.1 Create Kafka producer with segmentio/kafka-go
    - Define KafkaProducer interface
    - Implement async producer with delivery confirmations
    - Configure batching and compression
    - Add connection retry logic
    - _Requirements: 3.1, 3.2, 3.3, 3.4_
  
  - [ ]* 6.2 Write property test for async non-blocking behavior
    - **Property 7: Async Producer Non-Blocking Behavior**
    - **Validates: Requirements 3.2**
  
  - [ ]* 6.3 Write unit tests for Kafka producer
    - Test message publishing with mock broker
    - Test delivery confirmation handling
    - Test error handling and retries
    - _Requirements: 3.3, 3.4_

- [ ] 7. Implement Watcher: MongoDB change stream watcher
  - [ ] 7.1 Create change stream watcher
    - Define ChangeStreamWatcher interface
    - Implement MongoDB connection with retry
    - Implement change stream monitoring
    - Extract operation type and document data
    - Handle resume token from events
    - _Requirements: 1.1, 1.2, 1.3, 1.4_
  
  - [ ]* 7.2 Write property test for change event capture
    - **Property 1: Change Event Capture Completeness**
    - **Validates: Requirements 1.1**
  
  - [ ]* 7.3 Write property test for data extraction correctness
    - **Property 2: Change Event Data Extraction Correctness**
    - **Validates: Requirements 1.3**
  
  - [ ]* 7.4 Write unit tests for change stream watcher
    - Test connection with mock MongoDB driver
    - Test event parsing for insert, update, replace
    - Test reconnection on stream failure
    - _Requirements: 1.1, 1.3, 1.4_

- [ ] 8. Implement Watcher: Message serialization
  - [ ] 8.1 Create change event types and JSON serialization
    - Define ChangeEvent struct with JSON tags
    - Implement serialization to JSON
    - Implement deserialization from JSON
    - _Requirements: 3.5_
  
  - [ ]* 8.2 Write property test for serialization round-trip
    - **Property 9: Message Serialization Round-Trip**
    - **Validates: Requirements 3.5, 4.3**

- [ ] 9. Integrate Watcher: Main event loop
  - [ ] 9.1 Wire together Watcher components
    - Initialize all dependencies with config
    - Implement main event processing loop
    - Connect change stream → Kafka → token storage
    - Ensure token save happens after Kafka confirmation
    - _Requirements: 2.1, 3.3_
  
  - [ ]* 9.2 Write property test for token save after Kafka success
    - **Property 4: Resume Token Save After Kafka Success**
    - **Validates: Requirements 2.1**
  
  - [ ]* 9.3 Write property test for Kafka confirmation before token save
    - **Property 8: Kafka Confirmation Before Token Save**
    - **Validates: Requirements 3.3**
  
  - [ ]* 9.4 Write property test for token retry on failure
    - **Property 5: Resume Token Retry on Failure**
    - **Validates: Requirements 2.4**

- [ ] 10. Implement Watcher: Graceful shutdown
  - [ ] 10.1 Add signal handling and graceful shutdown
    - Catch SIGTERM and SIGINT signals
    - Stop change stream consumption
    - Wait for in-flight Kafka writes
    - Save current resume token
    - Close all connections
    - _Requirements: 9.1, 9.4, 9.5_
  
  - [ ]* 10.2 Write property test for shutdown token save
    - **Property 20: Graceful Shutdown Token Save**
    - **Validates: Requirements 9.4**
  
  - [ ]* 10.3 Write unit tests for graceful shutdown
    - Test signal handling
    - Test token save on shutdown
    - Test exit code
    - _Requirements: 9.1, 9.4, 9.5_

- [ ] 11. Checkpoint - Watcher service complete
  - Ensure all Watcher tests pass, ask the user if questions arise.

- [ ] 12. Implement Syncer: Kafka consumer
  - [ ] 12.1 Create Kafka consumer with segmentio/kafka-go
    - Define KafkaConsumer interface
    - Implement consumer with consumer group
    - Implement message consumption loop
    - Implement offset commit logic
    - Add connection retry logic
    - _Requirements: 4.1, 4.2, 4.5_
  
  - [ ]* 12.2 Write property test for offset commit after write
    - **Property 11: Offset Commit After Database Write**
    - **Validates: Requirements 4.5**
  
  - [ ]* 12.3 Write unit tests for Kafka consumer
    - Test message consumption with mock broker
    - Test offset commit
    - Test consumer group rebalancing
    - _Requirements: 4.2, 4.5_

- [ ] 13. Implement Syncer: Batch buffer
  - [ ] 13.1 Create batch buffer with flush conditions
    - Define BatchBuffer interface
    - Implement buffer with size and time-based flush
    - Add thread-safe operations
    - Implement buffer clear after flush
    - _Requirements: 5.1, 5.2, 5.3, 5.4_
  
  - [ ]* 13.2 Write property test for message buffering
    - **Property 12: Message Buffering Before Flush**
    - **Validates: Requirements 5.1**
  
  - [ ]* 13.3 Write property test for buffer clear after flush
    - **Property 13: Buffer Clear After Flush**
    - **Validates: Requirements 5.4**
  
  - [ ]* 13.4 Write unit tests for batch buffer
    - Test size-based flush (1000 messages)
    - Test time-based flush (500ms)
    - Test buffer clear after flush
    - _Requirements: 5.2, 5.3, 5.4_

- [ ] 14. Implement Syncer: PostgreSQL writer
  - [ ] 14.1 Create PostgreSQL writer with pgx
    - Define PostgresWriter interface
    - Initialize connection pool with pgx/v5
    - Implement batch insert with prepared statements
    - Implement COPY protocol for large batches
    - Implement UPSERT with ON CONFLICT DO UPDATE
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 8.1_
  
  - [ ]* 14.2 Write property test for batch write protocol selection
    - **Property 15: Batch Write Protocol Selection**
    - **Validates: Requirements 7.4**
  
  - [ ]* 14.3 Write property test for connection pool wait
    - **Property 16: Connection Pool Wait on Exhaustion**
    - **Validates: Requirements 7.5**
  
  - [ ]* 14.4 Write property test for idempotent writes
    - **Property 17: Idempotent Write Operations**
    - **Validates: Requirements 8.2, 8.5**
  
  - [ ]* 14.5 Write property test for upsert logging
    - **Property 18: Upsert Operation Logging**
    - **Validates: Requirements 8.4**
  
  - [ ]* 14.6 Write unit tests for PostgreSQL writer
    - Test batch insert with test database
    - Test COPY protocol with large batch
    - Test UPSERT with duplicate records
    - Test connection pool configuration
    - _Requirements: 7.2, 7.4, 8.2_

- [ ] 15. Implement Syncer: Worker pool
  - [ ] 15.1 Create worker pool for parallel processing
    - Define WorkerPool interface
    - Implement worker goroutines
    - Implement work distribution via channels
    - Add graceful shutdown for workers
    - _Requirements: 6.1, 6.2, 6.4_
  
  - [ ]* 15.2 Write property test for worker message distribution
    - **Property 14: Worker Message Distribution**
    - **Validates: Requirements 6.2**
  
  - [ ]* 15.3 Write unit tests for worker pool
    - Test worker initialization
    - Test message distribution
    - Test worker shutdown
    - _Requirements: 6.1, 6.2_

- [ ] 16. Implement Syncer: Error handling
  - [ ] 16.1 Add error handling for consumption and writes
    - Handle deserialization errors (skip and continue)
    - Handle database write errors (retry or skip)
    - Handle connection errors (retry with backoff)
    - Implement error classification
    - _Requirements: 4.4, 12.5_
  
  - [ ]* 16.2 Write property test for consumer error resilience
    - **Property 10: Consumer Error Resilience**
    - **Validates: Requirements 4.4**
  
  - [ ]* 16.3 Write unit tests for error handling
    - Test deserialization error handling
    - Test database error handling
    - Test connection error handling
    - _Requirements: 4.4_

- [ ] 17. Integrate Syncer: Main processing loop
  - [ ] 17.1 Wire together Syncer components
    - Initialize all dependencies with config
    - Start worker pool
    - Implement main consumption loop
    - Connect Kafka → workers → batch buffer → PostgreSQL
    - Implement flush timer
    - _Requirements: 5.1, 5.2, 5.3_
  
  - [ ]* 17.2 Write integration tests for Syncer
    - Test end-to-end message flow
    - Test batch flushing
    - Test worker coordination
    - _Requirements: 5.1, 5.4_

- [ ] 18. Implement Syncer: Graceful shutdown
  - [ ] 18.1 Add signal handling and graceful shutdown
    - Catch SIGTERM and SIGINT signals
    - Stop Kafka consumption
    - Signal workers to finish
    - Flush all buffers to PostgreSQL
    - Commit final offsets
    - Close all connections
    - _Requirements: 9.2, 9.3, 9.5_
  
  - [ ]* 18.2 Write property test for shutdown buffer flush
    - **Property 19: Graceful Shutdown Buffer Flush**
    - **Validates: Requirements 9.3**
  
  - [ ]* 18.3 Write unit tests for graceful shutdown
    - Test signal handling
    - Test buffer flush on shutdown
    - Test exit code
    - _Requirements: 9.2, 9.3, 9.5_

- [ ] 19. Checkpoint - Syncer service complete
  - Ensure all Syncer tests pass, ask the user if questions arise.

- [ ] 20. Implement logging enhancements
  - [ ] 20.1 Add structured logging throughout both services
    - Add contextual fields to all log statements
    - Implement error logging with stack traces
    - Add log level configuration
    - _Requirements: 10.3, 10.5_
  
  - [ ]* 20.2 Write property test for log field completeness
    - **Property 21: Structured Log Field Completeness**
    - **Validates: Requirements 10.3**
  
  - [ ]* 20.3 Write property test for error log stack traces
    - **Property 22: Error Log Stack Trace Inclusion**
    - **Validates: Requirements 10.5**

- [ ] 21. Add metrics and observability
  - [ ] 21.1 Implement Prometheus metrics endpoints
    - Add metrics for Watcher (events processed, publish latency, errors)
    - Add metrics for Syncer (messages consumed, batch size, flush latency, errors)
    - Expose /metrics endpoint for both services
    - _Requirements: Design monitoring section_
  
  - [ ]* 21.2 Write unit tests for metrics
    - Test metric collection
    - Test metric endpoint
    - _Requirements: Design monitoring section_

- [ ] 22. Add health check endpoints
  - [ ] 22.1 Implement health check endpoints
    - Add /health endpoint for Watcher (checks change stream status)
    - Add /health endpoint for Syncer (checks consumer status)
    - Return appropriate HTTP status codes
    - _Requirements: Design deployment section_
  
  - [ ]* 22.2 Write unit tests for health checks
    - Test health endpoint responses
    - Test health status determination
    - _Requirements: Design deployment section_

- [ ] 23. Create Docker configuration
  - [ ] 23.1 Create Dockerfiles for both services
    - Multi-stage build for minimal image size
    - Use Go 1.21+ base image
    - Copy only necessary binaries
    - Set appropriate entrypoints
    - _Requirements: Design deployment section_

- [ ] 24. Create Kubernetes manifests
  - [ ] 24.1 Create Kubernetes deployment manifests
    - StatefulSet for Watcher
    - Deployment for Syncer
    - ConfigMaps for configuration
    - Secrets for credentials
    - Service definitions
    - _Requirements: Design deployment section_

- [ ] 25. Create configuration examples
  - [ ] 25.1 Create example configuration files
    - watcher.yaml with all configuration options
    - syncer.yaml with all configuration options
    - Add comments explaining each option
    - _Requirements: 11.1, 11.2_

- [ ] 26. Create README and documentation
  - [ ] 26.1 Write comprehensive README
    - System overview and architecture
    - Setup and installation instructions
    - Configuration guide
    - Running the services
    - Monitoring and troubleshooting
    - _Requirements: Design overview_

- [ ] 27. Final checkpoint - Complete system
  - Run end-to-end integration tests with real MongoDB, Kafka, and PostgreSQL
  - Verify zero data loss with resume tokens
  - Verify idempotency with duplicate messages
  - Verify graceful shutdown behavior
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation at major milestones
- Property tests validate universal correctness properties (minimum 100 iterations each)
- Unit tests validate specific examples and edge cases
- Integration tests validate end-to-end flows
- The implementation follows a bottom-up approach: shared modules → service-specific modules → integration → observability
