# Requirements Document

## Introduction

This document specifies the requirements for a high-performance, fault-tolerant Change Data Capture (CDC) pipeline for a gaming backend. The system consists of two Go services that form a complete CDC pipeline: Service B (Watcher) monitors MongoDB change streams and publishes events to Kafka, while Service C (Syncer) consumes Kafka messages and writes them to PostgreSQL. The system prioritizes zero data loss, high throughput, and fault tolerance.

## Glossary

- **Watcher**: Service B that monitors MongoDB change streams and produces messages to Kafka
- **Syncer**: Service C that consumes Kafka messages and writes to PostgreSQL
- **Resume_Token**: MongoDB change stream token that allows resuming from a specific point in the change stream
- **Change_Stream**: MongoDB feature that provides real-time notifications of data changes
- **Batch_Buffer**: In-memory collection of messages waiting to be written to PostgreSQL
- **Worker_Pool**: Collection of goroutines processing messages in parallel
- **CDC_Pipeline**: The complete system consisting of Watcher and Syncer services
- **Idempotent_Write**: Database write operation that produces the same result regardless of how many times it's executed

## Requirements

### Requirement 1: MongoDB Change Stream Monitoring

**User Story:** As a system operator, I want the Watcher to monitor MongoDB change streams, so that all data changes are captured in real-time.

#### Acceptance Criteria

1. WHEN MongoDB data changes occur, THE Watcher SHALL capture insert, update, and replace operations
2. WHEN the Watcher starts, THE Watcher SHALL connect to MongoDB using the go.mongodb.org/mongo-driver library
3. WHEN a change stream event is received, THE Watcher SHALL extract the operation type and document data
4. WHEN the change stream connection fails, THE Watcher SHALL attempt to reconnect with exponential backoff

### Requirement 2: Resume Token Persistence

**User Story:** As a system operator, I want the Watcher to persist resume tokens, so that zero data loss is guaranteed across service restarts.

#### Acceptance Criteria

1. WHEN a Kafka message is successfully published, THE Watcher SHALL save the corresponding resume token to persistent storage
2. WHEN the Watcher starts, THE Watcher SHALL load the last saved resume token and resume the change stream from that position
3. IF no resume token exists, THEN THE Watcher SHALL start from the current point in the change stream
4. WHEN saving a resume token fails, THE Watcher SHALL log the error and retry the save operation
5. THE Watcher SHALL support both Redis and local file storage for resume token persistence

### Requirement 3: Kafka Message Production

**User Story:** As a system operator, I want the Watcher to publish change events to Kafka with high performance, so that the pipeline maintains low latency.

#### Acceptance Criteria

1. WHEN a change stream event is received, THE Watcher SHALL publish it to Kafka using the segmentio/kafka-go library
2. THE Watcher SHALL use async producer logic to avoid blocking on Kafka writes
3. WHEN a Kafka write completes successfully, THE Watcher SHALL receive delivery confirmation before saving the resume token
4. WHEN a Kafka write fails, THE Watcher SHALL retry the write with exponential backoff
5. THE Watcher SHALL serialize change events to JSON format before publishing to Kafka

### Requirement 4: Kafka Message Consumption

**User Story:** As a system operator, I want the Syncer to consume Kafka messages efficiently, so that the pipeline maintains high throughput.

#### Acceptance Criteria

1. WHEN the Syncer starts, THE Syncer SHALL connect to Kafka using the segmentio/kafka-go library
2. WHEN Kafka messages are available, THE Syncer SHALL consume them continuously
3. THE Syncer SHALL deserialize JSON messages into structured data types
4. WHEN message consumption fails, THE Syncer SHALL log the error and continue processing subsequent messages
5. THE Syncer SHALL commit Kafka offsets only after successful PostgreSQL writes

### Requirement 5: Batch Insert Optimization

**User Story:** As a system operator, I want the Syncer to batch database writes, so that PostgreSQL I/O is minimized and throughput is maximized.

#### Acceptance Criteria

1. WHEN the Syncer receives messages, THE Syncer SHALL buffer them in memory until a flush condition is met
2. WHEN the buffer contains 1000 messages, THE Syncer SHALL flush the buffer to PostgreSQL
3. WHEN 500 milliseconds have elapsed since the last flush, THE Syncer SHALL flush the buffer to PostgreSQL
4. WHEN a flush operation completes, THE Syncer SHALL clear the buffer and commit Kafka offsets
5. THE Syncer SHALL use a single batch INSERT statement for all buffered messages

### Requirement 6: Worker Pool Parallelism

**User Story:** As a system operator, I want the Syncer to process messages in parallel, so that CPU resources are fully utilized.

#### Acceptance Criteria

1. WHEN the Syncer starts, THE Syncer SHALL create a configurable number of worker goroutines
2. WHEN messages are consumed from Kafka, THE Syncer SHALL distribute them across worker goroutines
3. THE Syncer SHALL use channels to coordinate work distribution between workers
4. WHEN a worker completes processing, THE Syncer SHALL assign it the next available message
5. THE Syncer SHALL ensure thread-safe access to shared resources across workers

### Requirement 7: PostgreSQL High-Performance Writes

**User Story:** As a system operator, I want the Syncer to write to PostgreSQL with maximum performance, so that the pipeline can handle high message volumes.

#### Acceptance Criteria

1. WHEN the Syncer starts, THE Syncer SHALL connect to PostgreSQL using the jackc/pgx/v5 library
2. THE Syncer SHALL configure a connection pool with a minimum of 10 and maximum of 50 connections
3. WHEN writing batches, THE Syncer SHALL use prepared statements for optimal performance
4. THE Syncer SHALL use the COPY protocol for bulk inserts when batch size exceeds 100 messages
5. WHEN connection pool exhaustion occurs, THE Syncer SHALL wait for available connections rather than failing

### Requirement 8: Idempotent Write Operations

**User Story:** As a system operator, I want the Syncer to handle duplicate messages gracefully, so that data consistency is maintained.

#### Acceptance Criteria

1. WHEN inserting records, THE Syncer SHALL use ON CONFLICT DO UPDATE clauses to implement upsert semantics
2. WHEN a duplicate message is processed, THE Syncer SHALL update the existing record with the latest data
3. THE Syncer SHALL define a unique constraint on the primary key or natural key for conflict detection
4. WHEN an upsert completes, THE Syncer SHALL log whether an insert or update occurred
5. THE Syncer SHALL ensure that processing the same message multiple times produces identical database state

### Requirement 9: Graceful Shutdown

**User Story:** As a system operator, I want both services to shut down gracefully, so that no data is lost during deployment or restart.

#### Acceptance Criteria

1. WHEN a SIGTERM or SIGINT signal is received, THE Watcher SHALL stop consuming from the change stream
2. WHEN a SIGTERM or SIGINT signal is received, THE Syncer SHALL stop consuming from Kafka
3. WHEN the Syncer shuts down, THE Syncer SHALL flush all buffered messages to PostgreSQL before exiting
4. WHEN the Watcher shuts down, THE Watcher SHALL save the current resume token before exiting
5. WHEN shutdown completes, THE services SHALL log a shutdown confirmation message and exit with status code 0

### Requirement 10: Structured Logging

**User Story:** As a system operator, I want both services to provide structured logs, so that I can monitor and debug the pipeline effectively.

#### Acceptance Criteria

1. THE Watcher SHALL use uber-go/zap for zero-allocation structured logging
2. THE Syncer SHALL use uber-go/zap for zero-allocation structured logging
3. WHEN logging events, THE services SHALL include contextual fields such as timestamp, service name, and operation type
4. THE services SHALL support configurable log levels (debug, info, warn, error)
5. WHEN errors occur, THE services SHALL log stack traces and error context

### Requirement 11: Configuration Management

**User Story:** As a system operator, I want both services to support flexible configuration, so that I can deploy them across different environments.

#### Acceptance Criteria

1. THE Watcher SHALL load configuration from environment variables or configuration files
2. THE Syncer SHALL load configuration from environment variables or configuration files
3. THE services SHALL support viper or godotenv for configuration management
4. WHEN required configuration is missing, THE services SHALL log an error and exit with a non-zero status code
5. THE services SHALL validate configuration values on startup and reject invalid configurations

### Requirement 12: Error Handling and Retry Logic

**User Story:** As a system operator, I want both services to handle transient errors gracefully, so that the pipeline remains resilient.

#### Acceptance Criteria

1. WHEN transient errors occur, THE services SHALL retry operations with exponential backoff
2. THE services SHALL implement a maximum retry count to prevent infinite retry loops
3. WHEN maximum retries are exceeded, THE services SHALL log a fatal error and alert operators
4. WHEN network errors occur, THE services SHALL wait before reconnecting to external dependencies
5. THE services SHALL distinguish between retryable and non-retryable errors

### Requirement 13: Modular Code Architecture

**User Story:** As a developer, I want both services to follow SOLID principles, so that the codebase is maintainable and extensible.

#### Acceptance Criteria

1. THE services SHALL separate concerns into distinct modules (config, logging, database, messaging)
2. THE services SHALL use interfaces to define contracts between modules
3. THE services SHALL use dependency injection to provide implementations to modules
4. WHEN adding new functionality, THE services SHALL extend existing interfaces rather than modifying them
5. THE services SHALL use strict typing and avoid interface{} except where necessary for serialization
