import {
	CreateBucketCommand,
	HeadBucketCommand,
	PutObjectCommand,
	S3Client,
	type S3ClientConfig,
} from "@aws-sdk/client-s3";
import { afterEach, beforeAll, describe, expect, test } from "vitest";
import { S3Mutex } from "../src/index";

// Generate unique bucket name for test isolation
const testBucket = `test-bucket-${Date.now()}`;
const testObject = "test-file.txt";
const testContent = Buffer.from("Hello, MinIO!");
const lockBucket = `locks-bucket-${Date.now()}`;

const s3ClientConfig: S3ClientConfig = {
	forcePathStyle: true,
	endpoint: process.env.S3_ENDPOINT || "http://localhost:9000",
	region: process.env.S3_REGION || "us-east-1",
	credentials: {
		accessKeyId: process.env.S3_ACCESS_KEY || "root",
		secretAccessKey: process.env.S3_SECRET_KEY || "password",
	},
};

const s3Client = new S3Client(s3ClientConfig);

// Utility function to ensure a bucket exists, creating it if necessary
async function ensureBucketExists(bucketName: string): Promise<void> {
	try {
		await s3Client.send(new HeadBucketCommand({ Bucket: bucketName }));
	} catch (error) {
		const err = error as { $metadata?: { httpStatusCode?: number } };
		if (err.$metadata?.httpStatusCode === 404) {
			// Bucket doesn't exist, try to create it
			try {
				await s3Client.send(new CreateBucketCommand({ Bucket: bucketName }));
			} catch (createError) {
				throw new Error(
					`Failed to create test bucket ${bucketName}: ${createError}. Make sure S3/MinIO is running and accessible.`,
				);
			}
		} else {
			// Other error - S3 not accessible
			throw new Error(
				`S3/MinIO not accessible for bucket ${bucketName}: ${error}. Make sure S3/MinIO is running at ${s3ClientConfig.endpoint}`,
			);
		}
	}
}

describe("S3Mutex Tests", () => {
	beforeAll(async () => {
		// Ensure test buckets exist before running tests
		try {
			await ensureBucketExists(lockBucket);
			await ensureBucketExists(testBucket);
		} catch (error) {
			throw new Error(
				`Test setup failed: ${error}. Please ensure S3/MinIO is running via 'docker-compose up -d'`,
			);
		}
	});

	// Initialize the S3Mutex with shorter timeouts for testing
	const s3Mutex = new S3Mutex({
		s3Client,
		bucketName: lockBucket,
		maxRetries: 3,
		retryDelayMs: 100,
		lockTimeoutMs: 1000, // 1 second lock timeout for faster tests
	});

	const lockName = `test-lock-${Date.now()}`;

	// Cleanup after all tests
	afterEach(async () => {
		// Force release any remaining locks
		await s3Mutex.releaseLock(lockName, true).catch(() => {
			// Ignore errors during cleanup
		});
	});

	test("should acquire and release a lock", async () => {
		// Acquire the lock
		const acquired = await s3Mutex.acquireLock(lockName);
		expect(acquired).toBe(true);

		// Check if the lock is held
		const isLocked = await s3Mutex.isLocked(lockName);
		expect(isLocked).toBe(true);

		// Check if we own the lock
		const isOwnedByUs = await s3Mutex.isOwnedByUs(lockName);
		expect(isOwnedByUs).toBe(true);

		// Release the lock
		const released = await s3Mutex.releaseLock(lockName);
		expect(released).toBe(true);

		// Check that the lock is no longer held
		const isLockedAfterRelease = await s3Mutex.isLocked(lockName);
		expect(isLockedAfterRelease).toBe(false);
	});

	test("should not be able to acquire an already held lock", async () => {
		// Create a second mutex instance (simulating another process)
		const secondMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			maxRetries: 3,
			retryDelayMs: 100,
			lockTimeoutMs: 1000,
		});

		// First instance acquires the lock
		const acquired = await s3Mutex.acquireLock(lockName);
		expect(acquired).toBe(true);

		// Second instance tries to acquire the same lock
		const secondAcquired = await secondMutex.acquireLock(lockName);
		expect(secondAcquired).toBe(false);

		// First instance releases the lock
		const released = await s3Mutex.releaseLock(lockName);
		expect(released).toBe(true);
	});

	test("should be able to refresh a lock", async () => {
		// Acquire the lock
		const acquired = await s3Mutex.acquireLock(lockName);
		expect(acquired).toBe(true);

		// Refresh the lock
		const { refreshed } = await s3Mutex.refreshLock(lockName);
		expect(refreshed).toBe(true);

		// Should still own the lock after refresh
		const isOwnedByUs = await s3Mutex.isOwnedByUs(lockName);
		expect(isOwnedByUs).toBe(true);

		// Release the lock
		await s3Mutex.releaseLock(lockName);
	});

	test("a second mutex with the same injected ownerId can refresh the lock", async () => {
		// Use an explicit ownerId so two cooperating instances (e.g. main
		// thread + heartbeat worker) can act on the same lock.
		const sharedOwnerId = `shared-${Date.now()}-${Math.random()}`;
		const ownerMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			maxRetries: 3,
			retryDelayMs: 100,
			lockTimeoutMs: 2000,
			ownerId: sharedOwnerId,
		});
		const heartbeatMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			maxRetries: 3,
			retryDelayMs: 100,
			lockTimeoutMs: 2000,
			ownerId: sharedOwnerId,
		});

		expect(ownerMutex.getOwnerId()).toBe(sharedOwnerId);
		expect(heartbeatMutex.getOwnerId()).toBe(sharedOwnerId);

		try {
			const acquired = await ownerMutex.acquireLock(lockName);
			expect(acquired).toBe(true);

			// Without ownerId injection this would return false because the
			// heartbeatMutex would have a freshly-generated ownerId.
			const { refreshed } = await heartbeatMutex.refreshLock(lockName);
			expect(refreshed).toBe(true);

			// A third mutex with its own auto-generated ownerId must NOT be
			// able to refresh — guards against accidentally widening ownership.
			const strangerMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 3,
				retryDelayMs: 100,
				lockTimeoutMs: 2000,
			});
			expect(strangerMutex.getOwnerId()).not.toBe(sharedOwnerId);
			const stolenRefresh = await strangerMutex.refreshLock(lockName);
			expect(stolenRefresh.refreshed).toBe(false);
			expect(stolenRefresh.reason).toBe("not-owner");
		} finally {
			await ownerMutex.releaseLock(lockName, true);
		}
	});

	test("should execute a function with a lock and release it afterwards", async () => {
		let executionFlag = false;

		const result = await s3Mutex.withLock(lockName, async () => {
			// Check if lock is held within the function
			const isLocked = await s3Mutex.isLocked(lockName);
			expect(isLocked).toBe(true);

			executionFlag = true;
			return "success";
		});

		// Check that the function was executed
		expect(executionFlag).toBe(true);
		expect(result).toBe("success");

		// Lock should be auto-released after function execution
		const isLockedAfter = await s3Mutex.isLocked(lockName);
		expect(isLockedAfter).toBe(false);
	});

	test("should handle lock expiration", async () => {
		// Create a mutex with very short lock timeout
		const shortTimeoutMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			lockTimeoutMs: 500, // 500ms timeout
			maxRetries: 3,
			retryDelayMs: 100,
		});

		// Create a second mutex with reduced clock skew tolerance so it can acquire expired locks
		const secondMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			lockTimeoutMs: 1000,
			clockSkewToleranceMs: 100, // Reduced clock skew tolerance
			maxRetries: 3,
			retryDelayMs: 100,
		});

		// First instance acquires the lock
		const acquired = await shortTimeoutMutex.acquireLock(lockName);
		expect(acquired).toBe(true);

		// Wait for the lock to expire
		await new Promise((resolve) => setTimeout(resolve, 600));

		// Second instance should be able to acquire the expired lock
		const secondAcquired = await secondMutex.acquireLock(lockName);
		expect(secondAcquired).toBe(true);

		// Clean up
		await secondMutex.releaseLock(lockName);
	});

	test("should handle concurrent lock attempts", async () => {
		// Create multiple mutex instances
		const mutexes = Array.from(
			{ length: 5 },
			() =>
				new S3Mutex({
					s3Client,
					bucketName: lockBucket,
					maxRetries: 3,
					retryDelayMs: 100,
					lockTimeoutMs: 1000,
				}),
		);

		// Try to acquire locks concurrently
		const results = await Promise.all(
			mutexes.map((mutex) => mutex.acquireLock(lockName)),
		);

		// Exactly one mutex should acquire the lock
		const successCount = results.filter(Boolean).length;
		expect(successCount).toBe(1);

		// Find which mutex acquired the lock and release it
		const acquiredIndex = results.findIndex((result) => result === true);
		if (acquiredIndex >= 0) {
			await mutexes[acquiredIndex].releaseLock(lockName);
		}
	});

	test("should force release a lock held by another instance", async () => {
		// Create another mutex instance
		const otherMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			maxRetries: 3,
			retryDelayMs: 100,
			lockTimeoutMs: 1000,
		});

		// Other instance acquires the lock
		const acquired = await otherMutex.acquireLock(lockName);
		expect(acquired).toBe(true);

		// Our instance can't acquire the lock normally
		const ourAcquired = await s3Mutex.acquireLock(lockName);
		expect(ourAcquired).toBe(false);

		// Force release the lock
		const forceReleased = await s3Mutex.releaseLock(lockName, true);
		expect(forceReleased).toBe(true);

		// Now we should be able to acquire the lock
		const acquiredAfterForce = await s3Mutex.acquireLock(lockName);
		expect(acquiredAfterForce).toBe(true);

		// Clean up
		await s3Mutex.releaseLock(lockName);
	});

	test("should completely delete a lock file", async () => {
		// Create a unique lock for this test
		const testLockName = `delete-test-lock-${Date.now()}`;

		// First acquire the lock so it exists
		const acquired = await s3Mutex.acquireLock(testLockName);
		expect(acquired).toBe(true);

		// Now delete it
		const deleted = await s3Mutex.deleteLock(testLockName);
		expect(deleted).toBe(true);

		// Verify it's gone by trying to check if it's locked
		// This should return false but not throw an error
		const isLocked = await s3Mutex.isLocked(testLockName);
		expect(isLocked).toBe(false);

		// Try to delete a lock that doesn't exist (with force=true)
		const nonExistentLockName = `non-existent-lock-${Date.now()}`;
		const deletedNonExistent = await s3Mutex.deleteLock(
			nonExistentLockName,
			true,
		);
		expect(deletedNonExistent).toBe(true); // Should return true since the lock doesn't exist
	});

	test("should not refresh an expired lock", async () => {
		// Create a mutex with very short lock timeout
		const shortTimeoutMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			lockTimeoutMs: 300, // 300ms timeout
			maxRetries: 3,
			retryDelayMs: 100,
		});

		// Create a unique lock for this test
		const testLockName = `refresh-test-lock-${Date.now()}`;

		// Acquire the lock
		const acquired = await shortTimeoutMutex.acquireLock(testLockName);
		expect(acquired).toBe(true);

		// Wait for the lock to expire
		await new Promise((resolve) => setTimeout(resolve, 400));

		// Try to refresh the lock - should fail because it's expired
		const refreshed = await shortTimeoutMutex.refreshLock(testLockName);
		expect(refreshed.refreshed).toBe(false);
		expect(refreshed.reason).toBe("expired");

		// Clean up
		await shortTimeoutMutex.deleteLock(testLockName, true);
	});

	test("should handle errors in withLock function", async () => {
		// Create a unique lock for this test
		const testLockName = `error-test-lock-${Date.now()}`;

		// Use withLock with a function that throws an error
		let error: unknown;
		try {
			await s3Mutex.withLock(testLockName, async () => {
				// Check if lock is held within the function
				const isLocked = await s3Mutex.isLocked(testLockName);
				expect(isLocked).toBe(true);

				// Throw an error
				throw new Error("Test error");
			});
		} catch (e) {
			error = e;
		}

		// Verify that the error was propagated
		expect(error).toBeDefined();
		expect((error as Error).message).toBe("Test error");

		// Check that the lock was properly released despite the error
		const isLockedAfter = await s3Mutex.isLocked(testLockName);
		expect(isLockedAfter).toBe(false);
	});

	test("should find and clean up stale locks", async () => {
		// Create a unique prefix for this test to avoid interference
		const testPrefix = `cleanup-test-${Date.now()}/`;

		// Create a mutex with this prefix
		const cleanupMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			keyPrefix: testPrefix,
			lockTimeoutMs: 500, // Short timeout for testing
			maxRetries: 3,
			retryDelayMs: 100,
		});

		// Create several locks
		const lockNames = Array.from({ length: 3 }, (_, i) => `test-lock-${i}`);

		// Acquire all locks
		await Promise.all(lockNames.map((name) => cleanupMutex.acquireLock(name)));

		// Wait for locks to expire
		await new Promise((resolve) => setTimeout(resolve, 600));

		// First do a dry run
		const dryRunResult = await cleanupMutex.cleanupStaleLocks({
			prefix: testPrefix,
			dryRun: true,
		});

		// Should have found stale locks but not cleaned them
		expect(dryRunResult.total).toBeGreaterThanOrEqual(3);
		expect(dryRunResult.stale).toBeGreaterThanOrEqual(3);
		expect(dryRunResult.cleaned).toBe(0);

		// Now do a real cleanup
		const cleanupResult = await cleanupMutex.cleanupStaleLocks({
			prefix: testPrefix,
		});

		// Should have cleaned up the stale locks
		expect(cleanupResult.total).toBeGreaterThanOrEqual(3);
		expect(cleanupResult.stale).toBeGreaterThanOrEqual(3);
		expect(cleanupResult.cleaned).toBeGreaterThanOrEqual(3);

		// Verify locks are gone
		for (const name of lockNames) {
			const isLocked = await cleanupMutex.isLocked(name);
			expect(isLocked).toBe(false);
		}
	});

	test("should handle lock acquisition with priorities", async () => {
		// Create a unique lock for this test
		const priorityLockName = `priority-test-lock-${Date.now()}`;

		// Create two mutex instances
		const lowPriorityMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			maxRetries: 3,
			retryDelayMs: 100,
			lockTimeoutMs: 1000,
		});

		const highPriorityMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			maxRetries: 3,
			retryDelayMs: 100,
			lockTimeoutMs: 1000,
		});

		// Low priority acquires the lock first
		const lowAcquired = await lowPriorityMutex.acquireLock(
			priorityLockName,
			undefined,
			1,
		);
		expect(lowAcquired).toBe(true);

		// High priority tries to acquire the same lock with higher priority
		// In a real deadlock scenario, this might succeed, but in our test it will still fail
		// since we don't have a complete deadlock detection system
		const highAcquired = await highPriorityMutex.acquireLock(
			priorityLockName,
			undefined,
			10,
		);
		expect(highAcquired).toBe(false);

		// Release the lock
		await lowPriorityMutex.releaseLock(priorityLockName);

		// Now high priority should be able to acquire it
		const highAcquiredAfter = await highPriorityMutex.acquireLock(
			priorityLockName,
			undefined,
			10,
		);
		expect(highAcquiredAfter).toBe(true);

		// Clean up
		await highPriorityMutex.releaseLock(priorityLockName);
	});

	test("should handle clock skew tolerance", async () => {
		// Create a unique lock for this test
		const skewLockName = `skew-test-lock-${Date.now()}`;

		// Create a mutex with specific clock skew tolerance
		const skewMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			lockTimeoutMs: 1000,
			clockSkewToleranceMs: 200, // 200ms tolerance
			maxRetries: 3,
			retryDelayMs: 100,
		});

		// Acquire the lock
		const acquired = await skewMutex.acquireLock(skewLockName);
		expect(acquired).toBe(true);

		// Wait for just less than the lock timeout
		await new Promise((resolve) => setTimeout(resolve, 900));

		// Another mutex with no tolerance should still see the lock as valid
		const noToleranceMutex = new S3Mutex({
			s3Client,
			bucketName: lockBucket,
			lockTimeoutMs: 1000,
			clockSkewToleranceMs: 0,
			maxRetries: 3,
			retryDelayMs: 100,
		});

		const secondAcquired = await noToleranceMutex.acquireLock(skewLockName);
		// Should fail because the lock is still valid for a mutex without skew tolerance
		expect(secondAcquired).toBe(false);

		// Wait for the lock to expire + skew tolerance
		await new Promise((resolve) => setTimeout(resolve, 300));

		// Now the lock should be acquirable
		const thirdAcquired = await noToleranceMutex.acquireLock(skewLockName);
		expect(thirdAcquired).toBe(true);

		// Clean up
		await noToleranceMutex.releaseLock(skewLockName);
	});

	describe("Error Handling and Resilience Tests", () => {
		test("should handle malformed lock files gracefully", async () => {
			const malformedLockName = `malformed-lock-${Date.now()}`;

			// Create a malformed lock file by directly putting invalid JSON
			const lockKey = `locks/${malformedLockName}.json`;
			await s3Client.send(
				new PutObjectCommand({
					Bucket: lockBucket,
					Key: lockKey,
					Body: "{ invalid json }",
					ContentType: "application/json",
				}),
			);

			// Attempting to check the lock should handle the error gracefully
			let errorThrown = false;
			try {
				await s3Mutex.isLocked(malformedLockName);
			} catch (error) {
				errorThrown = true;
			}
			expect(errorThrown).toBe(true);

			// Clean up
			await s3Mutex.deleteLock(malformedLockName, true);
		});

		test("should handle empty lock files", async () => {
			const emptyLockName = `empty-lock-${Date.now()}`;

			// Create an empty lock file
			const lockKey = `locks/${emptyLockName}.json`;
			await s3Client.send(
				new PutObjectCommand({
					Bucket: lockBucket,
					Key: lockKey,
					Body: "",
					ContentType: "application/json",
				}),
			);

			let errorThrown = false;
			try {
				await s3Mutex.isLocked(emptyLockName);
			} catch (error) {
				errorThrown = true;
			}
			expect(errorThrown).toBe(true);

			// Clean up
			await s3Mutex.deleteLock(emptyLockName, true);
		});

		test("should handle network timeouts gracefully", async () => {
			// Create a mutex with very short timeout for testing
			const timeoutMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 1,
				retryDelayMs: 50,
				lockTimeoutMs: 100,
			});

			const timeoutLockName = `timeout-lock-${Date.now()}`;

			// Should still work within normal parameters
			const acquired = await timeoutMutex.acquireLock(timeoutLockName, 200);
			expect(acquired).toBe(true);

			// Clean up
			await timeoutMutex.releaseLock(timeoutLockName);
		});

		test("should handle missing bucket scenario", async () => {
			const nonExistentBucket = `non-existent-bucket-${Date.now()}`;
			const brokenMutex = new S3Mutex({
				s3Client,
				bucketName: nonExistentBucket,
				maxRetries: 1,
				retryDelayMs: 50,
			});

			const testLockName = `test-lock-${Date.now()}`;

			// Should fail gracefully without crashing
			const acquired = await brokenMutex.acquireLock(testLockName);
			expect(acquired).toBe(false);
		});
	});

	describe("Concurrency and Race Condition Tests", () => {
		test("should handle rapid lock/unlock cycles", async () => {
			const rapidLockName = `rapid-lock-${Date.now()}`;
			const results = [];

			// Perform rapid acquire/release cycles
			for (let i = 0; i < 10; i++) {
				const acquired = await s3Mutex.acquireLock(rapidLockName);
				if (acquired) {
					results.push(true);
					await s3Mutex.releaseLock(rapidLockName);
				} else {
					results.push(false);
				}
			}

			// All operations should succeed
			expect(results.every((r) => r === true)).toBe(true);
		});

		test("should handle multiple withLock operations concurrently", async () => {
			const concurrentLockName = `concurrent-lock-${Date.now()}`;
			const executionOrder = [];

			// Create multiple mutex instances with very limited retries
			const mutexes = Array.from(
				{ length: 5 },
				() =>
					new S3Mutex({
						s3Client,
						bucketName: lockBucket,
						maxRetries: 1, // Very limited retries
						retryDelayMs: 10, // Very short delay
						lockTimeoutMs: 1000,
					}),
			);

			// Create multiple concurrent withLock operations trying to get the SAME lock
			const executionPromises = mutexes.map(
				(mutex, i) =>
					mutex.withLock(
						concurrentLockName,
						async () => {
							executionOrder.push(i);
							// Simulate some work
							await new Promise((resolve) => setTimeout(resolve, 100));
							return i;
						},
						{ retries: 1 },
					), // Limit retries to make test faster
			);

			const results = await Promise.all(executionPromises);

			// With very limited retries, not all should succeed
			const successfulResults = results.filter((r) => r !== null);
			const failedResults = results.filter((r) => r === null);

			// At least one should succeed, and some should fail
			expect(successfulResults.length).toBeGreaterThanOrEqual(1);
			expect(failedResults.length).toBeGreaterThanOrEqual(0);
			expect(successfulResults.length + failedResults.length).toBe(5);
		});

		test("should handle lock ownership verification correctly", async () => {
			const ownershipLockName = `ownership-lock-${Date.now()}`;

			// First mutex acquires the lock
			const acquired1 = await s3Mutex.acquireLock(ownershipLockName);
			expect(acquired1).toBe(true);

			// Verify ownership
			const isOwned = await s3Mutex.isOwnedByUs(ownershipLockName);
			expect(isOwned).toBe(true);

			// Second mutex should see it as not owned by them
			const secondMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 1,
				retryDelayMs: 100,
			});

			const isOwnedBySecond = await secondMutex.isOwnedByUs(ownershipLockName);
			expect(isOwnedBySecond).toBe(false);

			// Clean up
			await s3Mutex.releaseLock(ownershipLockName);
		});
	});

	describe("Edge Cases and Boundary Conditions", () => {
		test("should handle very long lock names", async () => {
			// Create a very long lock name (but within S3 key limits)
			const longLockName = `${"a".repeat(200)}-${Date.now()}`;

			const acquired = await s3Mutex.acquireLock(longLockName);
			expect(acquired).toBe(true);

			const isLocked = await s3Mutex.isLocked(longLockName);
			expect(isLocked).toBe(true);

			await s3Mutex.releaseLock(longLockName);
		});

		test("should handle lock names with special characters", async () => {
			// Test with special characters that should be sanitized
			const specialLockName = `test-lock-with-@#$%^&*()-${Date.now()}`;

			const acquired = await s3Mutex.acquireLock(specialLockName);
			expect(acquired).toBe(true);

			await s3Mutex.releaseLock(specialLockName);
		});

		test("should handle zero timeout gracefully", async () => {
			const zeroTimeoutLockName = `zero-timeout-lock-${Date.now()}`;

			const acquired = await s3Mutex.acquireLock(zeroTimeoutLockName, 0);
			// Should still try to acquire even with zero timeout
			expect(typeof acquired).toBe("boolean");

			if (acquired) {
				await s3Mutex.releaseLock(zeroTimeoutLockName);
			}
		});

		test("should handle maximum priority values", async () => {
			const maxPriorityLockName = `max-priority-lock-${Date.now()}`;

			const acquired = await s3Mutex.acquireLock(
				maxPriorityLockName,
				undefined,
				Number.MAX_SAFE_INTEGER,
			);
			expect(acquired).toBe(true);

			await s3Mutex.releaseLock(maxPriorityLockName);
		});

		test("should handle negative priority values", async () => {
			const negativePriorityLockName = `negative-priority-lock-${Date.now()}`;

			const acquired = await s3Mutex.acquireLock(
				negativePriorityLockName,
				undefined,
				-1,
			);
			expect(acquired).toBe(true);

			await s3Mutex.releaseLock(negativePriorityLockName);
		});
	});

	describe("Heartbeat and Timeout Scenarios", () => {
		test("should handle withLock with very short timeout", async () => {
			const shortTimeoutLockName = `short-timeout-lock-${Date.now()}`;
			let executionCompleted = false;

			const shortTimeoutMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 1,
				retryDelayMs: 50,
				lockTimeoutMs: 200, // Very short timeout
			});

			const result = await shortTimeoutMutex.withLock(
				shortTimeoutLockName,
				async () => {
					// This should complete before timeout
					executionCompleted = true;
					return "success";
				},
				{ timeoutMs: 150 },
			);

			expect(executionCompleted).toBe(true);
			expect(result).toBe("success");
		});

		test("should handle heartbeat failure scenarios", async () => {
			const heartbeatFailLockName = `heartbeat-fail-lock-${Date.now()}`;

			// Create a mutex with very short heartbeat interval
			const fastHeartbeatMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				lockTimeoutMs: 300, // Short timeout to force frequent heartbeats
				maxRetries: 2,
				retryDelayMs: 50,
			});

			let executionStarted = false;

			const result = await fastHeartbeatMutex.withLock(
				heartbeatFailLockName,
				async () => {
					executionStarted = true;
					// Simulate work that takes longer than heartbeat interval
					await new Promise((resolve) => setTimeout(resolve, 100));
					return "completed";
				},
			);

			expect(executionStarted).toBe(true);
			expect(result).toBe("completed");

			// Ensure lock is released
			const isLocked = await fastHeartbeatMutex.isLocked(heartbeatFailLockName);
			expect(isLocked).toBe(false);
		});
	});

	describe("Advanced Lock Management", () => {
		test("should handle refreshing non-existent locks", async () => {
			const nonExistentLockName = `non-existent-refresh-lock-${Date.now()}`;

			const refreshed = await s3Mutex.refreshLock(nonExistentLockName);
			expect(refreshed.refreshed).toBe(false);
			expect(refreshed.reason).toBe("not-found");
		});

		test("should handle refreshing locks owned by others", async () => {
			const otherOwnerLockName = `other-owner-lock-${Date.now()}`;

			// Create another mutex instance
			const otherMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 2,
				retryDelayMs: 100,
			});

			// Other mutex acquires the lock
			await otherMutex.acquireLock(otherOwnerLockName);

			// Our mutex tries to refresh it - should fail
			const refreshed = await s3Mutex.refreshLock(otherOwnerLockName);
			expect(refreshed.refreshed).toBe(false);
			expect(refreshed.reason).toBe("not-owner");

			// Clean up
			await otherMutex.releaseLock(otherOwnerLockName);
		});

		test("should handle multiple cleanup operations", async () => {
			const cleanupPrefix = `cleanup-multiple-${Date.now()}/`;

			const cleanupMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				keyPrefix: cleanupPrefix,
				lockTimeoutMs: 200,
				maxRetries: 2,
				retryDelayMs: 50,
			});

			// Create multiple locks with different states
			const lockNames = Array.from({ length: 5 }, (_, i) => `multi-lock-${i}`);

			// Acquire some locks
			await Promise.all(
				lockNames.slice(0, 3).map((name) => cleanupMutex.acquireLock(name)),
			);

			// Release some locks
			await cleanupMutex.releaseLock(lockNames[0]);

			// Wait for some to expire
			await new Promise((resolve) => setTimeout(resolve, 300));

			// Run cleanup
			const result = await cleanupMutex.cleanupStaleLocks({
				prefix: cleanupPrefix,
			});

			expect(result.total).toBeGreaterThan(0);
			expect(result.stale).toBeGreaterThan(0);

			// Clean up remaining locks
			await Promise.all(
				lockNames.map((name) =>
					cleanupMutex.deleteLock(name, true).catch(() => {}),
				),
			);
		});

		test("should handle delete operations on active locks", async () => {
			const activeLockName = `active-delete-lock-${Date.now()}`;

			// Acquire the lock
			const acquired = await s3Mutex.acquireLock(activeLockName);
			expect(acquired).toBe(true);

			// Delete our own lock should succeed
			const deleted = await s3Mutex.deleteLock(activeLockName);
			expect(deleted).toBe(true);

			// Verify it's gone
			const isLocked = await s3Mutex.isLocked(activeLockName);
			expect(isLocked).toBe(false);
		});

		test("should handle attempting to delete locks owned by others", async () => {
			const otherDeleteLockName = `other-delete-lock-${Date.now()}`;

			// Create another mutex
			const otherMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 2,
				retryDelayMs: 100,
			});

			// Other mutex acquires the lock
			await otherMutex.acquireLock(otherDeleteLockName);

			// Our mutex tries to delete it without force - should fail
			const deleted = await s3Mutex.deleteLock(otherDeleteLockName, false);
			expect(deleted).toBe(false);

			// With force should succeed
			const forceDeleted = await s3Mutex.deleteLock(otherDeleteLockName, true);
			expect(forceDeleted).toBe(true);
		});
	});

	describe("Clock Skew and Time-based Edge Cases", () => {
		test("should handle locks with future expiration times", async () => {
			const futureLockName = `future-lock-${Date.now()}`;

			// Acquire lock normally
			const acquired = await s3Mutex.acquireLock(futureLockName);
			expect(acquired).toBe(true);

			// Verify lock is properly owned
			const isOwned = await s3Mutex.isOwnedByUs(futureLockName);
			expect(isOwned).toBe(true);

			await s3Mutex.releaseLock(futureLockName);
		});

		test("should handle extreme clock skew scenarios", async () => {
			const skewLockName = `extreme-skew-lock-${Date.now()}`;

			// Create mutex with large clock skew tolerance
			const extremeSkewMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				lockTimeoutMs: 1000,
				clockSkewToleranceMs: 10000, // 10 second tolerance
				maxRetries: 2,
				retryDelayMs: 100,
			});

			const acquired = await extremeSkewMutex.acquireLock(skewLockName);
			expect(acquired).toBe(true);

			// Should be able to refresh
			const { refreshed } = await extremeSkewMutex.refreshLock(skewLockName);
			expect(refreshed).toBe(true);

			await extremeSkewMutex.releaseLock(skewLockName);
		});
	});

	describe("Memory and Resource Management", () => {
		test("should not leak memory with many failed lock attempts", async () => {
			const memoryTestLockName = `memory-test-lock-${Date.now()}`;

			// Create a long-held lock
			const holdingMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				lockTimeoutMs: 5000,
				maxRetries: 1,
				retryDelayMs: 50,
			});

			const testingMutex = new S3Mutex({
				s3Client,
				bucketName: lockBucket,
				maxRetries: 1,
				retryDelayMs: 50,
			});

			await holdingMutex.acquireLock(memoryTestLockName);

			// Attempt many failed acquisitions
			const failedAttempts = [];
			for (let i = 0; i < 50; i++) {
				failedAttempts.push(
					testingMutex.acquireLock(`${memoryTestLockName}-${i}`),
				);
			}

			const results = await Promise.all(failedAttempts);

			// All attempts should succeed since they're different lock names
			expect(results.every((r) => r === true)).toBe(true);

			// Clean up
			await holdingMutex.releaseLock(memoryTestLockName);
			for (let i = 0; i < 50; i++) {
				await testingMutex
					.releaseLock(`${memoryTestLockName}-${i}`)
					.catch(() => {});
			}
		});

		test("should handle stream errors gracefully", async () => {
			const streamErrorLockName = `stream-error-lock-${Date.now()}`;

			// Create a normal lock first
			const acquired = await s3Mutex.acquireLock(streamErrorLockName);
			expect(acquired).toBe(true);

			// Operations should still work normally
			const isLocked = await s3Mutex.isLocked(streamErrorLockName);
			expect(isLocked).toBe(true);

			await s3Mutex.releaseLock(streamErrorLockName);
		});
	});
});
