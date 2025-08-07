# S3-Mutex

A distributed locking mechanism for Node.js applications using AWS S3 as the backend storage.

## Features

- **Distributed locking**: Coordinate access across multiple services
- **Automatic bucket creation**: Optionally create S3 buckets if they don't exist
- **Deadlock detection**: Priority-based mechanism for deadlock resolution
- **Timeout handling**: Automatic lock expiration with configurable timeouts
- **Lock heartbeat**: Automatic lock refresh during long operations
- **Retry with backoff and jitter**: Configurable retry mechanism
- **Error handling**: Specific handling for S3 service issues
- **Cleanup utilities**: Tools for managing stale locks

> **⚠️ Warning**: S3-based locking has significant limitations compared to purpose-built locking solutions. S3 operations have higher latency and are not optimized for high-frequency lock operations. Consider alternatives like Redis, DynamoDB, or ZooKeeper for mission-critical applications.

## Installation

```bash
npm install s3-mutex
# or
yarn add s3-mutex
# or
pnpm add s3-mutex
```

## Usage

### Basic usage

```typescript
import { S3Client } from "@aws-sdk/client-s3";
import { S3Mutex } from "s3-mutex";

// Option 1: Initialize with existing S3 client
const s3Client = new S3Client({
  region: "us-east-1",
  // other configuration options
});

const mutex = new S3Mutex({
  s3Client,
  bucketName: "my-locks-bucket",
  keyPrefix: "locks/", // optional, defaults to "locks/"
});

// Option 2: Let S3Mutex create the S3 client
const mutex2 = new S3Mutex({
  bucketName: "my-locks-bucket",
  s3ClientConfig: {
    region: "us-east-1",
    forcePathStyle: true, // useful for MinIO/LocalStack
    // other S3ClientConfig options
  },
});

// Option 3: Automatically create bucket if it doesn't exist
const mutex3 = new S3Mutex({
  bucketName: "my-locks-bucket",
  createBucketIfNotExists: true, // Create bucket automatically
  s3ClientConfig: {
    region: "us-east-1",
    // other S3ClientConfig options
  },
});

// Acquire a lock
const acquired = await mutex.acquireLock("my-resource-lock");
if (acquired) {
  try {
    // Do work with the exclusive lock
    await doSomething();
  } finally {
    // Release the lock when done
    await mutex.releaseLock("my-resource-lock");
  }
} else {
  console.log("Failed to acquire lock");
}
```

### Using the withLock helper

The `withLock` helper method simplifies working with locks by automatically releasing them:

```typescript
// Execute a function with an automatic lock
const result = await mutex.withLock("my-resource-lock", async () => {
  // This function is executed only when the lock is acquired
  const data = await processResource();
  return data;
});

if (result === null) {
  // Lock acquisition failed
  console.log("Could not acquire lock");
} else {
  // Lock was acquired, function executed, and lock released
  console.log("Process completed with result:", result);
}
```

## Configuration Options

```typescript
const mutex = new S3Mutex({
  // Required: bucket name
  bucketName: "my-locks-bucket",
  
  // Either provide an existing S3 client
  s3Client: s3Client,
  
  // OR provide S3 client configuration (s3-mutex will create the client)
  s3ClientConfig: {
    region: "us-east-1",
    forcePathStyle: true,       // Useful for MinIO/LocalStack
    endpoint: "http://localhost:9000", // For local development
    credentials: {
      accessKeyId: "your-key",
      secretAccessKey: "your-secret"
    }
  },
  
  // Optional configuration with defaults
  createBucketIfNotExists: false,  // Create bucket if it doesn't exist
  keyPrefix: "locks/",          // Prefix for lock keys in S3
  maxRetries: 5,                // Max number of acquisition attempts
  retryDelayMs: 200,            // Base delay between retries (exponential backoff)
  maxRetryDelayMs: 5000,        // Max delay between retries
  useJitter: true,              // Add randomness to retry delays
  lockTimeoutMs: 60000,         // Lock expiration (1 minute)
  clockSkewToleranceMs: 1000,   // Tolerance for clock differences
});
```

## API Reference

### Constructor

```typescript
new S3Mutex(options: S3MutexOptions)
```

### Methods

- **acquireLock(lockName: string, timeoutMs?: number, priority?: number): Promise<boolean>**: Acquire a named lock with optional timeout and priority
- **releaseLock(lockName: string, force?: boolean): Promise<boolean>**: Release a lock, with optional force parameter
- **refreshLock(lockName: string): Promise<boolean>**: Refresh a lock's expiration time
- **isLocked(lockName: string): Promise<boolean>**: Check if a lock is currently held and not expired
- **isOwnedByUs(lockName: string): Promise<boolean>**: Check if we own a specific lock
- **deleteLock(lockName: string, force?: boolean): Promise<boolean>**: Completely remove a lock file from S3
- **withLock<T>(lockName: string, fn: () => Promise<T>, options?: {timeoutMs?: number, retries?: number}): Promise<T | null>**: Execute a function with an automatic lock
- **cleanupStaleLocks(options?: {prefix?: string, olderThan?: number, dryRun?: boolean}): Promise<{cleaned: number, total: number, stale: number}>**: Find and clean up expired locks

### Lock Priority and Deadlock Prevention

S3-Mutex includes deadlock prevention through priority-based acquisition. When multiple processes attempt to acquire locks, those with higher priority values will be favored if deadlock conditions are detected.

```typescript
// Basic priority usage (higher value = higher priority)
const acquired = await mutex.acquireLock("resource-lock", undefined, 10);

// Example: High-priority background job
const backgroundJobLock = await mutex.acquireLock(
  "critical-maintenance",
  30000, // 30 second timeout
  100    // High priority
);

// Example: Low-priority routine task
const routineLock = await mutex.acquireLock(
  "routine-cleanup",
  10000, // 10 second timeout
  1      // Low priority
);
```

**How Priority Works:**
- When a deadlock is potentially detected, higher priority requests can force-acquire locks
- Priority only matters during deadlock resolution, not normal acquisition
- Use priorities strategically: critical operations get higher values, routine tasks get lower values

## Bucket Management

### Automatic Bucket Creation

S3-Mutex can automatically create the S3 bucket if it doesn't exist. This is particularly useful for development environments or when deploying to new AWS accounts.

```typescript
const mutex = new S3Mutex({
  bucketName: "my-locks-bucket",
  createBucketIfNotExists: true, // Enable automatic bucket creation
  s3ClientConfig: {
    region: "us-east-1",
  },
});

// The bucket will be created automatically on first use
const acquired = await mutex.acquireLock("my-resource-lock");
```

**Important Notes:**
- Bucket creation requires appropriate IAM permissions (`s3:CreateBucket`)
- If the bucket already exists, no error is thrown
- The bucket is created with default settings (no versioning, no lifecycle policies)
- For production use, consider creating buckets manually with proper configuration

### Manual Bucket Creation

For production environments, it's recommended to create buckets manually:

```bash
# Using AWS CLI
aws s3 mb s3://my-locks-bucket --region us-east-1

# Or using CloudFormation/Terraform for infrastructure as code
```

## Advanced Usage

### Handling Stale Locks

```typescript
// Find and clean up stale locks
const results = await mutex.cleanupStaleLocks({
  prefix: "locks/myapp/",  // Optional prefix to limit cleanup scope
  olderThan: Date.now() - 3600000,  // Optional custom age (default is lockTimeoutMs)
  dryRun: true,  // Optional: just report stale locks without deleting
});

console.log(`Found ${results.stale} stale locks out of ${results.total} total locks`);
console.log(`Cleaned up ${results.cleaned} locks`);

// Cleanup all stale locks with default settings
const quickCleanup = await mutex.cleanupStaleLocks();

// Cleanup locks older than 2 hours
const oldLockCleanup = await mutex.cleanupStaleLocks({
  olderThan: Date.now() - (2 * 60 * 60 * 1000)
});
```

### Force-releasing a Lock

```typescript
// Force release a lock (use with caution)
await mutex.releaseLock("resource-lock", true);

// Force delete a lock file completely
await mutex.deleteLock("resource-lock", true);

// Check lock ownership before operations
if (await mutex.isOwnedByUs("resource-lock")) {
  await mutex.refreshLock("resource-lock");
  // do work
  await mutex.releaseLock("resource-lock");
}
```

## Best Practices

1. **Set appropriate timeouts**: Configure lock timeouts that match your workload duration
2. **Handle failure gracefully**: Always check if lock acquisition was successful
3. **Use the withLock helper**: Ensures locks are always released, even if errors occur
4. **Implement proper error handling**: Be prepared for S3 service errors and throttling
5. **Run periodic cleanup**: Use the cleanupStaleLocks method to maintain your lock storage
6. **Consider performance implications**: S3 operations have higher latency than in-memory solutions
7. **Test thoroughly under load**: Verify lock reliability under your specific workload conditions
8. **Have a fallback strategy**: Plan for occasional lock failures in production environments
9. **Monitor lock contention**: High contention may indicate need for architectural changes
10. **Use appropriate priorities**: Reserve high priorities for critical operations, use low priorities for routine tasks
11. **Handle null returns from withLock**: The `withLock` method returns `null` if lock acquisition fails
12. **Consider clock skew**: Set `clockSkewToleranceMs` appropriately for your distributed environment

## Development and Testing

### Prerequisites

- Node.js 18+
- Docker (for running S3-compatible storage locally)

### Local Development Setup

1. **Start MinIO (S3-compatible storage) for testing:**

```bash
# Using Docker Compose (if available in the project)
docker-compose up -d

# Or run MinIO directly
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=root \
  -e MINIO_ROOT_PASSWORD=password \
  quay.io/minio/minio server /data --console-address ":9001"
```

2. **Install dependencies:**

```bash
pnpm install
```

3. **Run tests:**

```bash
# Run tests (requires MinIO running)
pnpm test

# Run tests with coverage
pnpm test:ci

# Build the project
pnpm build

# Lint code
pnpm lint
```

### Testing with Different S3 Implementations

The library is tested with:

- **MinIO** (recommended for local development)
- **LocalStack** (AWS services emulation)
- **AWS S3** (production)

#### Environment Variables for Testing

```bash
# S3 endpoint (default: http://localhost:9000)
S3_ENDPOINT=http://localhost:9000

# S3 region (default: us-east-1)
S3_REGION=us-east-1

# S3 credentials (defaults: root/password for MinIO)
S3_ACCESS_KEY=root
S3_SECRET_KEY=password
```

### Example Test Configuration

```typescript
import { S3Client } from "@aws-sdk/client-s3";
import { S3Mutex } from "s3-mutex";

// Test configuration for MinIO
const testMutex = new S3Mutex({
  bucketName: "test-locks-bucket",
  createBucketIfNotExists: true, // Automatically create test bucket
  s3ClientConfig: {
    forcePathStyle: true,
    endpoint: "http://localhost:9000",
    region: "us-east-1",
    credentials: {
      accessKeyId: "root",
      secretAccessKey: "password",
    },
  },
  // Faster settings for testing
  maxRetries: 3,
  retryDelayMs: 100,
  lockTimeoutMs: 1000,
});
```

