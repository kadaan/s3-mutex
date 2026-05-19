// src/index.ts
import {
	CreateBucketCommand,
	DeleteObjectCommand,
	GetObjectCommand,
	HeadBucketCommand,
	HeadObjectCommand,
	ListObjectsV2Command,
	PutObjectCommand,
	S3Client,
	type S3ClientConfig,
} from "@aws-sdk/client-s3";

// src/utils.ts
import type { Readable } from "node:stream";

/**
 * Converts a readable stream to a string
 */
export async function streamToString(
	stream: Readable | ReadableStream<Uint8Array> | Blob,
): Promise<string> {
	// Handle Blob
	if (stream instanceof Blob) {
		return await stream.text();
	}

	// Handle ReadableStream from browsers
	if ((stream as ReadableStream<Uint8Array>).getReader) {
		const reader = (stream as ReadableStream<Uint8Array>).getReader();
		const chunks: Uint8Array[] = [];

		while (true) {
			const { done, value } = await reader.read();
			if (done) {
				break;
			}
			chunks.push(value);
		}

		const concatenated = new Uint8Array(
			chunks.reduce((acc, chunk) => acc + chunk.length, 0),
		);
		let position = 0;

		for (const chunk of chunks) {
			concatenated.set(chunk, position);
			position += chunk.length;
		}

		return new TextDecoder().decode(concatenated);
	}

	// Handle Node.js Readable stream
	return new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		const readableStream = stream as Readable;

		const cleanup = () => {
			readableStream.removeAllListeners();
			if (typeof readableStream.destroy === "function") {
				readableStream.destroy();
			}
		};

		readableStream.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
		readableStream.on("error", (err) => {
			cleanup();
			reject(err);
		});
		readableStream.on("end", () => {
			cleanup();
			resolve(Buffer.concat(chunks).toString("utf8"));
		});
	});
}

export interface S3MutexOptions {
	/**
	 * AWS S3 client instance
	 */
	s3Client?: S3Client;

	s3ClientConfig?: S3ClientConfig;

	/**
	 * S3 bucket name where locks will be stored
	 */
	bucketName: string;

	/**
	 * Whether to create the bucket if it doesn't exist
	 * @default false
	 */
	createBucketIfNotExists?: boolean;

	/**
	 * Key prefix for lock files (optional)
	 */
	keyPrefix?: string;

	/**
	 * Max number of retries when acquiring a lock
	 * @default 5
	 */
	maxRetries?: number;

	/**
	 * Base delay between retries in milliseconds (used for exponential backoff)
	 * @default 200
	 */
	retryDelayMs?: number;

	/**
	 * Maximum delay between retries in milliseconds (caps the exponential backoff)
	 * @default 5000 (5 seconds)
	 */
	maxRetryDelayMs?: number;

	/**
	 * Whether to add jitter to retry delays to prevent synchronized retries
	 * @default true
	 */
	useJitter?: boolean;

	/**
	 * Lock timeout in milliseconds. After this time, the lock is considered stale
	 * and can be forcefully acquired by another process.
	 * @default 60000 (1 minute)
	 */
	lockTimeoutMs?: number;

	/**
	 * Clock skew tolerance in milliseconds. This value is added to expiration calculations
	 * to account for differences in system clocks between distributed processes.
	 * @default 1000 (1 second)
	 */
	clockSkewToleranceMs?: number;

	/**
	 * Identifier written into the lock's `owner` field. When omitted, a fresh
	 * id is generated as `${pid}-${Date.now()}-${random}`.
	 *
	 * Inject this when two `S3Mutex` instances need to operate on the same
	 * lock — for example, a heartbeat worker that refreshes a lock acquired
	 * by another thread. Both instances must be constructed with the same
	 * `ownerId` so `refreshLock` / `releaseLock`'s ownership check passes.
	 */
	ownerId?: string;
}

export interface LockInfo {
	locked: boolean;
	owner?: string;
	acquiredAt?: number;
	expiresAt?: number;
	priority?: number;
}

// Used for deadlock prevention
interface LockRequest {
	lockName: string;
	priority: number;
	acquiredAt: number;
	owner: string;
}

export class S3Mutex {
	private s3Client: S3Client;
	private bucketName: string;
	private keyPrefix: string;
	private maxRetries: number;
	private retryDelayMs: number;
	private maxRetryDelayMs: number;
	private useJitter: boolean;
	private lockTimeoutMs: number;
	private clockSkewToleranceMs: number;
	private ownerId: string;
	private createBucketIfNotExists: boolean;
	private bucketInitialized = false;
	private heldLocks: Map<string, string> = new Map(); // lockName -> etag
	private lockRequests: Map<string, LockRequest> = new Map(); // lockName -> request info
	// Track lock dependencies for deadlock detection - key: owner, value: set of lock names owner is waiting for
	private lockDependencies: Map<string, Set<string>> = new Map();

	constructor(options: S3MutexOptions) {
		this.s3Client =
			options.s3Client ??
			new S3Client({
				forcePathStyle: true,
				...options.s3ClientConfig,
			});
		this.bucketName = options.bucketName;
		this.keyPrefix = options.keyPrefix || "locks/";
		this.maxRetries = options.maxRetries || 5;
		this.retryDelayMs = options.retryDelayMs || 200;
		this.maxRetryDelayMs = options.maxRetryDelayMs || 5000; // 5 seconds default max
		this.useJitter = options.useJitter !== undefined ? options.useJitter : true;
		this.lockTimeoutMs = options.lockTimeoutMs || 60000; // 1 minute default
		this.clockSkewToleranceMs = options.clockSkewToleranceMs || 1000; // 1 second default
		this.createBucketIfNotExists = options.createBucketIfNotExists ?? false;
		this.ownerId =
			options.ownerId ??
			`${process.pid}-${Date.now()}-${Math.random().toString(36).substring(2, 15)}`;
	}

	/**
	 * Returns the identifier written into the `owner` field of locks acquired
	 * by this instance. Useful when one process needs to forward the id to a
	 * cooperating instance (e.g. a heartbeat worker) so refreshLock matches.
	 */
	public getOwnerId(): string {
		return this.ownerId;
	}

	/**
	 * Ensures the S3 bucket exists, creating it if necessary and configured to do so
	 */
	private async ensureBucketExists(): Promise<void> {
		if (this.bucketInitialized) {
			return;
		}

		try {
			// Check if the bucket exists
			await this.s3Client.send(
				new HeadBucketCommand({
					Bucket: this.bucketName,
				}),
			);

			this.bucketInitialized = true;
			return;
		} catch (error) {
			const err = error as {
				$metadata?: { httpStatusCode?: number };
				name?: string;
			};

			// If bucket doesn't exist and we're configured to create it
			if (
				(err.$metadata?.httpStatusCode === 404 ||
					err.name === "NoSuchBucket") &&
				this.createBucketIfNotExists
			) {
				try {
					await this.s3Client.send(
						new CreateBucketCommand({
							Bucket: this.bucketName,
						}),
					);

					this.bucketInitialized = true;
					return;
				} catch (createError) {
					const createErr = createError as {
						$metadata?: { httpStatusCode?: number };
						name?: string;
					};

					// If the bucket was already created by someone else, that's fine
					if (
						createErr.$metadata?.httpStatusCode === 409 ||
						createErr.name === "BucketAlreadyExists"
					) {
						this.bucketInitialized = true;
						return;
					}

					throw new Error(
						`Failed to create bucket ${this.bucketName}: ${(createError as Error).message}`,
					);
				}
			} else if (
				err.$metadata?.httpStatusCode === 404 ||
				err.name === "NoSuchBucket"
			) {
				// Bucket doesn't exist but we're not configured to create it
				throw new Error(
					`Bucket ${this.bucketName} does not exist. Set createBucketIfNotExists to true to create it automatically.`,
				);
			}

			// Re-throw other errors (access denied, etc.)
			throw new Error(
				`Failed to access bucket ${this.bucketName}: ${(error as Error).message}`,
			);
		}
	}

	/**
	 * Implements exponential backoff with optional jitter
	 * @param attempt The current attempt number (0-based)
	 * @returns A promise that resolves after the calculated delay
	 */
	private async exponentialBackoff(attempt: number): Promise<void> {
		// Calculate base delay with exponential backoff
		const baseDelay = Math.min(
			this.retryDelayMs * 2 ** attempt,
			this.maxRetryDelayMs,
		);

		// Add jitter to prevent synchronized retries (up to 30% variation)
		const jitter = this.useJitter ? Math.random() * 0.3 * baseDelay : 0;
		const delay = Math.floor(baseDelay + jitter);

		// Wait for the calculated delay
		await new Promise((resolve) => setTimeout(resolve, delay));
	}

	/**
	 * Formats the full S3 key for a lock
	 */
	private getLockKey(lockName: string): string {
		// Make sure the key doesn't contain invalid characters
		const sanitizedLockName = lockName.replace(/[^a-zA-Z0-9-_]/g, "_");
		return `${this.keyPrefix}${sanitizedLockName}.json`;
	}

	/**
	 * Utility function to clean an ETag by removing quotes
	 */
	private cleanETag(etag: string | undefined): string | undefined {
		return etag?.replace(/"/g, "");
	}

	/**
	 * Standard error handling for S3 operations
	 */
	private handleS3Error(
		error: unknown,
		operation: string,
		lockName: string,
	): never {
		const err = error as {
			$metadata?: { httpStatusCode?: number };
			name?: string;
			message: string;
		};

		if (err.$metadata?.httpStatusCode === 503) {
			throw new Error(
				`S3 service unavailable while ${operation} lock ${lockName}: ${err.message}`,
			);
		}
		if (err.name === "ThrottlingException") {
			throw new Error(
				`AWS request throttling encountered while ${operation} lock ${lockName}: ${err.message}`,
			);
		}

		const errorMessage = `Error ${operation} lock ${lockName}: ${err.message}`;
		const newError = new Error(errorMessage);
		// @ts-ignore - using cause property which might not be in Error type definition
		newError.cause = error;
		throw newError;
	}

	/**
	 * Atomically initializes a lock if it doesn't exist
	 * Uses a more atomic approach to initialize the lock file
	 */
	private async initializeLock(
		lockName: string,
	): Promise<{ initialized: boolean; etag?: string }> {
		const lockKey = this.getLockKey(lockName);

		try {
			// Try to directly create the lock with a condition that it doesn't exist
			// Using PutObject with conditional checks for atomicity
			const initialLockInfo: LockInfo = {
				locked: false,
			};

			const putResponse = await this.s3Client.send(
				new PutObjectCommand({
					Bucket: this.bucketName,
					Key: lockKey,
					Body: JSON.stringify(initialLockInfo),
					ContentType: "application/json",
					// Only create if the object doesn't exist (this is more atomic)
					// https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html
					IfNoneMatch: "*",
				}),
			);

			// If we get here, the lock was created
			return { initialized: true, etag: this.cleanETag(putResponse.ETag) };
		} catch (error) {
			// If the error is a 412 Precondition Failed, the lock already exists
			const err = error as { $metadata?: { httpStatusCode?: number } };
			if (err.$metadata?.httpStatusCode === 412) {
				// Get the existing lock info to return its ETag
				const headResponse = await this.s3Client.send(
					new HeadObjectCommand({
						Bucket: this.bucketName,
						Key: lockKey,
					}),
				);

				return {
					initialized: true,
					etag: this.cleanETag(headResponse.ETag),
				};
			}

			this.handleS3Error(error, "initializing", lockName);
		}
	}

	/**
	 * Gets the current state of a lock
	 */
	private async getLockInfo(
		lockName: string,
	): Promise<{ lockInfo: LockInfo; etag: string }> {
		const lockKey = this.getLockKey(lockName);

		try {
			const response = await this.s3Client.send(
				new GetObjectCommand({
					Bucket: this.bucketName,
					Key: lockKey,
				}),
			);

			if (!response.Body) {
				throw new Error(`Failed to get lock info for ${lockName}`);
			}

			if (!response.ETag) {
				throw new Error(`No ETag found for lock ${lockName}`);
			}

			const body = await streamToString(response.Body);
			const lockInfo = JSON.parse(body) as LockInfo;
			const etag = this.cleanETag(response.ETag) || "";

			return { lockInfo, etag };
		} catch (error) {
			const err = error as { $metadata?: { httpStatusCode?: number } };
			// Enhanced error handling for getLockInfo
			if (err.$metadata?.httpStatusCode === 404) {
				// Create a new error but preserve the original metadata for checking in isLocked/isOwnedByUs
				const notFoundError = new Error(
					`Lock file ${lockName} not found`,
				) as Error & { $metadata?: { httpStatusCode?: number } };
				notFoundError.$metadata = err.$metadata;
				throw notFoundError;
			}

			this.handleS3Error(error, "getting info for", lockName);
		}
	}

	/**
	 * Updates the lock info in S3 with improved error handling
	 */
	private async updateLockInfo(
		lockName: string,
		lockInfo: LockInfo,
		etag: string,
	): Promise<string | undefined> {
		const lockKey = this.getLockKey(lockName);

		try {
			const response = await this.s3Client.send(
				new PutObjectCommand({
					Bucket: this.bucketName,
					Key: lockKey,
					Body: JSON.stringify(lockInfo),
					ContentType: "application/json",
					IfMatch: etag,
				}),
			);

			return response.ETag?.replace(/"/g, "");
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			// Enhanced error handling with specific error types

			// If the error is a precondition failed, the ETag has changed
			if (error.$metadata?.httpStatusCode === 412) {
				return undefined; // ETag mismatch, lock was modified
			}

			// Handle specific AWS error types
			if (error.$metadata?.httpStatusCode === 503) {
				throw new Error(
					`S3 service unavailable while updating lock ${lockName}: ${error.message}`,
				);
			}
			if (error.name === "ThrottlingException") {
				throw new Error(
					`AWS request throttling encountered while updating lock ${lockName}: ${error.message}`,
				);
			}
			if (error.$metadata?.httpStatusCode === 404) {
				throw new Error(
					`Lock file ${lockName} disappeared during update operation`,
				);
			}
			if (
				error.name === "NetworkError" ||
				error.$metadata?.httpStatusCode >= 500
			) {
				throw new Error(
					`Network or server error while updating lock ${lockName}: ${error.message}`,
				);
			}

			// For unknown errors, add context and rethrow
			const errorMessage = `Error updating lock ${lockName}: ${error.message}`;
			const newError = new Error(errorMessage);
			newError.cause = error;
			throw newError;
		}
	}

	/**
	 * Registers a lock dependency to help with deadlock detection
	 * @param waitingOwner The owner waiting for the lock
	 * @param targetLockName The lock being waited for
	 * @param targetOwner The owner currently holding the lock
	 */
	private registerLockDependency(
		waitingOwner: string,
		targetLockName: string,
		targetOwner: string | undefined,
	): void {
		if (!targetOwner) {
			return;
		}

		// Get or create the set of locks this owner is waiting for
		let waitingFor = this.lockDependencies.get(waitingOwner);
		if (!waitingFor) {
			waitingFor = new Set<string>();
			this.lockDependencies.set(waitingOwner, waitingFor);
		}

		// Add the target lock to the set
		waitingFor.add(targetLockName);
	}

	/**
	 * Checks if there's a potential deadlock scenario by detecting circular wait conditions
	 * @param lockName The lock we're trying to acquire
	 * @param currentOwner The current owner of the lock
	 * @returns True if there's a potential deadlock
	 */
	private async isPotentialDeadlock(
		_lockName: string,
		currentOwner?: string,
	): Promise<boolean> {
		if (!currentOwner) {
			return false;
		}

		// If we're not waiting for any locks, there can't be a cycle
		if (this.lockRequests.size === 0) {
			return false;
		}

		// Check if the current owner is waiting for any locks we hold
		// To do this, we need to check for cycles in the dependency graph

		// Initialize the queue with the current owner
		const queue: string[] = [currentOwner];
		const visited = new Set<string>();

		while (queue.length > 0) {
			const owner = queue.shift();
			// Early return if somehow we get an undefined owner (shouldn't happen, but being safe)
			if (!owner) {
				continue;
			}

			// Skip already visited owners
			if (visited.has(owner)) {
				continue;
			}
			visited.add(owner);

			// Get the locks this owner is waiting for
			const waitingFor = this.lockDependencies.get(owner);
			if (!waitingFor) {
				continue;
			}

			// Check each lock the owner is waiting for
			for (const waitLockName of waitingFor) {
				// If the owner is waiting for a lock we hold, we have a cycle
				// Simply check our local heldLocks map instead of making S3 calls
				if (this.heldLocks.has(waitLockName)) {
					// We've detected a deadlock cycle
					return true;
				}

				// Find out who owns this lock and add them to the queue
				try {
					const { lockInfo } = await this.getLockInfo(waitLockName);
					if (lockInfo.locked && lockInfo.owner && lockInfo.owner !== owner) {
						queue.push(lockInfo.owner);
					}
				} catch (error) {
					// If we can't get lock info, continue with the next lock
					console.warn(
						`Error getting lock info for deadlock detection: ${error}`,
					);
				}
			}
		}

		return false;
	}

	/**
	 * Attempts to acquire a lock
	 * @param lockName Name of the lock to acquire
	 * @param timeoutMs Maximum time to wait for lock acquisition
	 * @param priority Optional priority for this lock request (higher values have higher priority)
	 * @returns A promise that resolves to true if the lock was acquired, false otherwise
	 */
	public async acquireLock(
		lockName: string,
		timeoutMs?: number,
		priority = 0,
	): Promise<boolean> {
		try {
			// Ensure the bucket exists before proceeding
			await this.ensureBucketExists();

			// Register this lock request for deadlock detection
			this.lockRequests.set(lockName, {
				lockName,
				priority,
				acquiredAt: Date.now(),
				owner: this.ownerId,
			});

			// Make sure the lock file exists (using atomic initialization)
			await this.initializeLock(lockName);

			const startTime = Date.now();
			const maxWaitTime = timeoutMs || this.lockTimeoutMs;

			for (let attempt = 0; attempt < this.maxRetries; attempt++) {
				// Check if we've exceeded the overall timeout
				if (Date.now() - startTime > maxWaitTime) {
					return false;
				}

				try {
					// Get the current lock info
					const { lockInfo, etag } = await this.getLockInfo(lockName);

					// Check if the lock is already held by us
					if (lockInfo.locked && lockInfo.owner === this.ownerId) {
						// We already own this lock
						this.heldLocks.set(lockName, etag);
						this.lockRequests.delete(lockName);
						return true;
					}

					// Register this lock dependency for deadlock detection
					if (lockInfo.locked && lockInfo.owner) {
						this.registerLockDependency(this.ownerId, lockName, lockInfo.owner);
					}

					// Check if the lock is unlocked or expired
					// Include clock skew tolerance in the expiration check
					const now = Date.now();
					const isLockFree =
						!lockInfo.locked ||
						(lockInfo.expiresAt !== undefined &&
							lockInfo.expiresAt + this.clockSkewToleranceMs < now);

					// If the lock is held but the current request has higher priority,
					// we'll try to force-acquire it if we detect a potential deadlock
					const canForceAcquire =
						lockInfo.locked &&
						lockInfo.priority !== undefined &&
						priority > lockInfo.priority &&
						(await this.isPotentialDeadlock(lockName, lockInfo.owner));

					if (isLockFree || canForceAcquire) {
						// Try to acquire the lock
						const newLockInfo: LockInfo = {
							locked: true,
							owner: this.ownerId,
							acquiredAt: now,
							expiresAt: now + this.lockTimeoutMs,
							priority: priority,
						};

						const newEtag = await this.updateLockInfo(
							lockName,
							newLockInfo,
							etag,
						);

						if (newEtag) {
							// We successfully acquired the lock
							this.heldLocks.set(lockName, newEtag);
							this.lockRequests.delete(lockName);
							// Clean up the dependency as we now own the lock
							const ourDependencies = this.lockDependencies.get(this.ownerId);
							if (ourDependencies) {
								ourDependencies.delete(lockName);
								if (ourDependencies.size === 0) {
									this.lockDependencies.delete(this.ownerId);
								}
							}
							return true;
						}
					}

					// Wait with exponential backoff before retrying
					await this.exponentialBackoff(attempt);
				} catch (error) {
					// Enhanced error handling
					const err = error as {
						$metadata?: { httpStatusCode?: number };
						name?: string;
						message: string;
					};
					if (err.$metadata?.httpStatusCode === 503) {
						console.warn(
							`S3 service unavailable while acquiring lock ${lockName}. Retrying...`,
						);
					} else if (err.name === "ThrottlingException") {
						console.warn(
							`AWS request throttling encountered while acquiring lock ${lockName}. Retrying...`,
						);
					} else if (err.$metadata?.httpStatusCode === 404) {
						// Lock file disappeared, try to initialize it again
						await this.initializeLock(lockName);
					} else {
						console.warn(
							`Error acquiring lock ${lockName}: ${err.message}. Retrying...`,
						);
					}

					// Use exponential backoff
					await this.exponentialBackoff(attempt);
				}
			}

			return false;
		} catch (error) {
			// Handle critical errors that prevent lock acquisition
			console.error(`Critical error acquiring lock ${lockName}:`, error);
			return false;
		} finally {
			// Always clean up lock requests and dependencies if lock acquisition failed
			// This prevents memory leaks from accumulating failed lock attempts
			if (!this.heldLocks.has(lockName)) {
				this.lockRequests.delete(lockName);

				// Clean up dependencies more carefully - only remove this specific lock dependency
				const ourDependencies = this.lockDependencies.get(this.ownerId);
				if (ourDependencies) {
					ourDependencies.delete(lockName);
					if (ourDependencies.size === 0) {
						this.lockDependencies.delete(this.ownerId);
					}
				}
			}
		}
	}

	/**
	 * Refreshes a lock's expiration time
	 * @param lockName Name of the lock to refresh
	 * @returns A promise that resolves to true if the lock was refreshed, false otherwise
	 */
	public async refreshLock(lockName: string): Promise<boolean> {
		try {
			// Get the current lock info
			const { lockInfo, etag } = await this.getLockInfo(lockName);

			// Check if the lock is held by us
			const now = Date.now();

			// Check if the lock is held by us AND not expired
			if (
				!lockInfo.locked ||
				lockInfo.owner !== this.ownerId ||
				(lockInfo.expiresAt !== undefined && lockInfo.expiresAt < now)
			) {
				return false;
			}

			// Update the expiration time
			lockInfo.expiresAt = now + this.lockTimeoutMs;

			const newEtag = await this.updateLockInfo(lockName, lockInfo, etag);

			return !!newEtag;
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			// Enhanced error handling
			if (error.$metadata?.httpStatusCode === 404) {
				console.warn(`Lock file ${lockName} not found during refresh`);
			} else if (error.$metadata?.httpStatusCode === 503) {
				console.warn(
					`S3 service unavailable while refreshing lock ${lockName}`,
				);
			} else {
				console.warn(`Error refreshing lock ${lockName}: ${error.message}`);
			}

			return false;
		}
	}

	/**
	 * Releases a lock
	 * @param lockName Name of the lock to release
	 * @param force If true, release the lock even if it's not owned by us
	 * @returns A promise that resolves to true if the lock was released, false otherwise
	 */
	public async releaseLock(lockName: string, force = false): Promise<boolean> {
		try {
			// Get the current lock info
			const { lockInfo, etag } = await this.getLockInfo(lockName);

			// Check if the lock is held by us or if we're forcing
			if (lockInfo.locked && !force && lockInfo.owner !== this.ownerId) {
				return false;
			}

			// Release the lock
			const newLockInfo: LockInfo = {
				locked: false,
			};

			const newEtag = await this.updateLockInfo(lockName, newLockInfo, etag);

			// Remove from our held locks
			this.heldLocks.delete(lockName);
			this.lockRequests.delete(lockName);

			// Clean up any dependencies related to this lock
			// For all owners waiting for this lock, remove it from their waiting set
			for (const [owner, waitingFor] of this.lockDependencies.entries()) {
				waitingFor.delete(lockName);
				if (waitingFor.size === 0) {
					this.lockDependencies.delete(owner);
				}
			}

			return !!newEtag;
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			// Enhanced error handling
			if (error.$metadata?.httpStatusCode === 404) {
				console.warn(`Lock file ${lockName} not found during release`);
			} else if (error.$metadata?.httpStatusCode === 503) {
				console.warn(`S3 service unavailable while releasing lock ${lockName}`);
			} else {
				console.warn(`Error releasing lock ${lockName}: ${error.message}`);
			}

			return false;
		}
	}

	/**
	 * Executes a function while holding a lock
	 * @param lockName Name of the lock to acquire
	 * @param fn Function to execute while holding the lock
	 * @param options Options for lock acquisition
	 * @returns A promise that resolves to the result of the function or null if the lock couldn't be acquired
	 */
	public async withLock<T>(
		lockName: string,
		fn: () => Promise<T>,
		options: { timeoutMs?: number; retries?: number } = {},
	): Promise<T | null> {
		const acquireTimeout = options.timeoutMs || this.lockTimeoutMs;
		const maxRetries = options.retries || this.maxRetries;

		// Try to acquire the lock
		for (let i = 0; i < maxRetries; i++) {
			const acquired = await this.acquireLock(lockName, acquireTimeout);

			if (acquired) {
				// Setup for heartbeat with atomic cleanup function
				let heartbeatInterval: NodeJS.Timeout | null = null;

				const stopHeartbeat = () => {
					if (heartbeatInterval) {
						clearInterval(heartbeatInterval);
						heartbeatInterval = null;
					}
				};

				try {
					// Set up a timer to refresh the lock periodically
					const refreshInterval = Math.max(this.lockTimeoutMs / 3, 1000);

					heartbeatInterval = setInterval(async () => {
						// Check if interval is still valid (prevents race conditions)
						if (!heartbeatInterval) {
							return;
						}

						try {
							const refreshed = await this.refreshLock(lockName);
							// If refresh fails, stop the heartbeat to prevent further attempts
							if (!refreshed) {
								console.warn(
									`Failed to refresh lock ${lockName}, stopping heartbeat`,
								);
								stopHeartbeat();
							}
						} catch (error) {
							console.warn(`Error refreshing lock ${lockName}: ${error}`);
							// Also stop on errors
							stopHeartbeat();
						}
					}, refreshInterval);

					// Execute the function
					const result = await fn();

					// Stop heartbeat before returning
					stopHeartbeat();

					// Return the result
					return result;
				} catch (error) {
					// Log the error and ensure it's propagated after cleanup
					console.error(
						`Error in function executed with lock ${lockName}:`,
						error,
					);

					// Stop heartbeat immediately
					stopHeartbeat();

					// Try to release the lock with timeout to prevent hanging
					const releaseWithTimeout = async (timeoutMs = 5000) => {
						const releasePromise = this.releaseLock(lockName);
						const timeoutPromise = new Promise((_, reject) =>
							setTimeout(() => reject(new Error("Release timeout")), timeoutMs),
						);

						try {
							await Promise.race([releasePromise, timeoutPromise]);
						} catch (releaseError) {
							console.warn(
								`Failed to release lock ${lockName}: ${releaseError}`,
							);
						}
					};

					await releaseWithTimeout();

					// Rethrow the original error
					throw error;
				} finally {
					// Clean up heartbeat
					stopHeartbeat();

					// Release the lock (this is also done in the catch block but we need it here for normal exit)
					try {
						await this.releaseLock(lockName);
					} catch (error) {
						console.warn(
							`Failed to release lock ${lockName} in finally block: ${error}`,
						);
					}
				}
			}

			// Wait before retrying
			await this.exponentialBackoff(i);
		}

		return null;
	}

	/**
	 * Check if a lock exists and is currently locked
	 * @param lockName Name of the lock to check
	 * @returns A promise that resolves to true if the lock exists and is locked, false otherwise
	 */
	public async isLocked(lockName: string): Promise<boolean> {
		try {
			const { lockInfo } = await this.getLockInfo(lockName);

			// Check if the lock is currently active
			const now = Date.now();
			return (
				lockInfo.locked && (!lockInfo.expiresAt || lockInfo.expiresAt > now)
			);
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			// If the lock doesn't exist, it's not locked
			if (error.$metadata?.httpStatusCode === 404) {
				return false;
			}

			// Re-throw other errors
			throw error;
		}
	}

	/**
	 * Check if we own a specific lock
	 * @param lockName Name of the lock to check
	 * @returns A promise that resolves to true if we own the lock, false otherwise
	 */
	public async isOwnedByUs(lockName: string): Promise<boolean> {
		try {
			const { lockInfo } = await this.getLockInfo(lockName);

			// Check if the lock is owned by us and not expired
			const now = Date.now();
			return (
				lockInfo.locked &&
				lockInfo.owner === this.ownerId &&
				(!lockInfo.expiresAt || lockInfo.expiresAt > now)
			);
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			// If the lock doesn't exist, we don't own it
			if (error.$metadata?.httpStatusCode === 404) {
				return false;
			}

			// Re-throw other errors
			throw error;
		}
	}

	/**
	 * Completely removes a lock file from S3
	 * @param lockName Name of the lock to delete
	 * @param force If true, delete the lock even if it's not owned by us
	 * @returns A promise that resolves to true if the lock was deleted, false otherwise
	 */
	public async deleteLock(lockName: string, force = false): Promise<boolean> {
		try {
			// Check if the lock is held by us or if we're forcing
			if (!force) {
				try {
					const isOwnedByUs = await this.isOwnedByUs(lockName);
					if (!isOwnedByUs) {
						return false;
					}
				} catch (error) {
					// If lock doesn't exist and we're not forcing, still try to delete to be consistent
					// This will catch the 404 below and return true
				}
			}

			const lockKey = this.getLockKey(lockName);

			// Delete the lock file
			await this.s3Client.send(
				new DeleteObjectCommand({
					Bucket: this.bucketName,
					Key: lockKey,
				}),
			);

			return true;
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			// Enhanced error handling
			if (error.$metadata?.httpStatusCode === 404) {
				// Already deleted
				return true;
			}

			if (error.$metadata?.httpStatusCode === 503) {
				console.warn(`S3 service unavailable while deleting lock ${lockName}`);
			} else {
				console.warn(`Error deleting lock ${lockName}: ${error.message}`);
			}

			return false;
		}
	}

	/**
	 * Finds and cleans up stale locks
	 * @param options Optional configuration for cleanup
	 * @returns A promise that resolves to the number of locks cleaned up
	 */
	public async cleanupStaleLocks(
		options: {
			prefix?: string;
			olderThan?: number;
			dryRun?: boolean;
		} = {},
	): Promise<{ cleaned: number; total: number; stale: number }> {
		// Ensure the bucket exists before proceeding
		await this.ensureBucketExists();

		const prefix = options.prefix || this.keyPrefix;
		const olderThan = options.olderThan || Date.now() - this.lockTimeoutMs;
		const dryRun = options.dryRun;

		let cleaned = 0;
		let total = 0;
		let stale = 0;
		let continuationToken: string | undefined;

		try {
			// List all locks in the bucket with the given prefix
			do {
				const response = await this.s3Client.send(
					new ListObjectsV2Command({
						Bucket: this.bucketName,
						Prefix: prefix,
						ContinuationToken: continuationToken,
					}),
				);

				continuationToken = response.NextContinuationToken;

				if (!response.Contents) {
					continue;
				}

				// Process each lock
				for (const item of response.Contents) {
					if (!item.Key) {
						continue;
					}

					total++;

					try {
						// Get the lock info
						const objResponse = await this.s3Client.send(
							new GetObjectCommand({
								Bucket: this.bucketName,
								Key: item.Key,
							}),
						);

						if (!objResponse.Body) {
							continue;
						}

						const body = await streamToString(objResponse.Body);
						const lockInfo = JSON.parse(body) as LockInfo;

						// Check if the lock is stale based on either acquisition time OR expiration
						const now = Date.now();
						const isStale =
							lockInfo.locked &&
							((lockInfo.acquiredAt !== undefined &&
								lockInfo.acquiredAt < olderThan) ||
								(lockInfo.expiresAt !== undefined && lockInfo.expiresAt < now));

						if (isStale) {
							stale++;

							// Delete stale lock if not in dry run mode
							if (!dryRun) {
								await this.s3Client.send(
									new DeleteObjectCommand({
										Bucket: this.bucketName,
										Key: item.Key,
									}),
								);
								cleaned++;
							}
						}
					} catch (error) {
						console.warn(`Error processing lock ${item.Key}: ${error}`);
					}
				}
			} while (continuationToken);

			return { cleaned, total, stale };
			// biome-ignore lint/suspicious/noExplicitAny: <explanation>
		} catch (error: any) {
			console.error(`Error cleaning up stale locks: ${error}`);
			return { cleaned, total, stale };
		}
	}
}
