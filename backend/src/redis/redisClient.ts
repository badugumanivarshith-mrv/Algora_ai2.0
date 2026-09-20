import Redis, { RedisOptions } from "ioredis";
import { config } from "../config/env";
import { logger } from "../utils/logger";

export interface RedisHealthMetrics {
  status: "connected" | "connecting" | "reconnecting" | "fallback_memory" | "error";
  isHealthy: boolean;
  latencyMs: number;
  uptimeSeconds?: number;
  usedMemoryBytes?: number;
  usedMemoryHuman?: string;
  connectedClients?: number;
  totalCommandsProcessed?: number;
  pubsubChannels?: number;
  keyCount?: number;
  role?: string;
}

export class RedisManager {
  private static instance: Redis | null = null;
  private static subscriberInstance: Redis | null = null;
  private static publisherInstance: Redis | null = null;
  private static isConnected: boolean = false;
  private static isAvailable: boolean = false;
  private static isInitialized: boolean = false;
  private static hasLoggedStatus: boolean = false;

  // In-memory key-value cache fallback if Redis is unreachable
  private static memoryFallback: Map<string, { value: string; expiresAt?: number }> = new Map();

  public static getOptions(disableRetry: boolean = false): RedisOptions {
    const baseOptions: RedisOptions = {
      maxRetriesPerRequest: disableRetry ? 1 : config.redisMaxRetriesPerRequest,
      connectTimeout: config.redisConnectTimeout || 1500,
      keyPrefix: config.redisKeyPrefix,
      lazyConnect: true,
      enableOfflineQueue: false,
      retryStrategy: disableRetry
        ? () => null
        : (times) => {
            if (times > 3) {
              return null; // Cap retries quickly to prevent log floods
            }
            return Math.min(times * 200, 1000);
          },
    };

    if (config.redisUrl) {
      return {
        ...baseOptions,
      };
    }

    return {
      ...baseOptions,
      host: config.redisHost,
      port: config.redisPort,
      password: config.redisPassword,
      db: config.redisDb,
    };
  }

  /**
   * Initializes primary, publisher, and subscriber Redis connections with graceful probe
   */
  public static async initialize(): Promise<void> {
    if (this.isInitialized) return;
    this.isInitialized = true;

    // Fast-path disable check
    if (process.env.REDIS_ENABLED === "false") {
      this.isAvailable = false;
      this.isConnected = false;
      this.logStatusOnce(false);
      return;
    }

    try {
      // Single connection probe with fast timeout
      const probeOptions = this.getOptions(true);
      const probeClient = config.redisUrl
        ? new Redis(config.redisUrl, probeOptions)
        : new Redis(probeOptions);

      // Suppress noisy uncaught errors on the probe client
      probeClient.on("error", () => {});

      await Promise.race([
        probeClient.connect(),
        new Promise((_, reject) => setTimeout(() => reject(new Error("Redis connection probe timeout")), 1000)),
      ]);

      await probeClient.ping();

      // Probe succeeded: Redis is reachable
      this.isAvailable = true;
      this.isConnected = true;
      this.instance = probeClient;

      // Attach event listeners for production resilience
      this.instance.on("error", (err) => {
        logger.debug(`[RedisManager] Primary connection error: ${err.message}`);
      });
      this.instance.on("close", () => {
        this.isConnected = false;
      });
      this.instance.on("ready", () => {
        this.isConnected = true;
      });

      // Initialize subscriber and publisher clients
      const standardOptions = this.getOptions(false);
      const createSubPubClient = (): Redis => {
        const c = config.redisUrl
          ? new Redis(config.redisUrl, standardOptions)
          : new Redis(standardOptions);
        c.on("error", () => {});
        c.connect().catch(() => {});
        return c;
      };

      this.publisherInstance = createSubPubClient();
      this.subscriberInstance = createSubPubClient();

      this.logStatusOnce(true);
    } catch {
      // Redis is not reachable; cleanly revert to memory fallback
      this.isAvailable = false;
      this.isConnected = false;
      this.instance = null;
      this.publisherInstance = null;
      this.subscriberInstance = null;
      this.logStatusOnce(false);
    }
  }

  private static logStatusOnce(isOnline: boolean): void {
    if (this.hasLoggedStatus) return;
    this.hasLoggedStatus = true;
    if (isOnline) {
      logger.info(`[Redis] Connected and ready (Host: ${config.redisHost || "URL"}).`);
    } else {
      logger.info(`[Redis] Redis unavailable.`);
      logger.info(`[Redis] Running in memory fallback mode.`);
    }
  }

  /**
   * Primary Redis client instance
   */
  public static getClient(): Redis {
    if (!this.instance && this.isAvailable) {
      this.initialize();
    }
    return this.instance!;
  }

  /**
   * Dedicated Publisher client instance
   */
  public static getPublisher(): Redis {
    if (!this.publisherInstance && this.isAvailable) {
      this.initialize();
    }
    return this.publisherInstance!;
  }

  /**
   * Dedicated Subscriber client instance
   */
  public static getSubscriber(): Redis {
    if (!this.subscriberInstance && this.isAvailable) {
      this.initialize();
    }
    return this.subscriberInstance!;
  }

  /**
   * Duplicate client for isolated tasks/queues
   */
  public static createDuplicateClient(name: string): Redis {
    const options = this.getOptions(false);
    const client = config.redisUrl ? new Redis(config.redisUrl, options) : new Redis(options);
    client.on("error", () => {});
    return client;
  }

  /**
   * Checks if Redis is ready
   */
  public static isReady(): boolean {
    return this.isConnected && this.instance?.status === "ready";
  }

  /**
   * Health metrics and telemetry
   */
  public static async getHealth(): Promise<RedisHealthMetrics> {
    const start = Date.now();
    if (!this.instance || this.instance.status !== "ready") {
      return {
        status: this.instance?.status as any || "fallback_memory",
        isHealthy: false,
        latencyMs: 0,
      };
    }

    try {
      const pong = await this.instance.ping();
      const latencyMs = Date.now() - start;

      let infoRaw = "";
      try {
        infoRaw = await this.instance.info();
      } catch {
        // ignore
      }

      const getVal = (regex: RegExp): string | undefined => {
        const match = infoRaw.match(regex);
        return match ? match[1].trim() : undefined;
      };

      const uptimeSeconds = Number(getVal(/uptime_in_seconds:(\d+)/)) || 0;
      const usedMemoryBytes = Number(getVal(/used_memory:(\d+)/)) || 0;
      const usedMemoryHuman = getVal(/used_memory_human:([^\r\n]+)/) || "0B";
      const connectedClients = Number(getVal(/connected_clients:(\d+)/)) || 1;
      const totalCommandsProcessed = Number(getVal(/total_commands_processed:(\d+)/)) || 0;
      const pubsubChannels = Number(getVal(/pubsub_channels:(\d+)/)) || 0;
      const role = getVal(/role:([^\r\n]+)/) || "master";

      let keyCount = 0;
      try {
        const dbsize = await this.instance.dbsize();
        keyCount = dbsize;
      } catch {
        keyCount = 0;
      }

      return {
        status: "connected",
        isHealthy: pong === "PONG",
        latencyMs,
        uptimeSeconds,
        usedMemoryBytes,
        usedMemoryHuman,
        connectedClients,
        totalCommandsProcessed,
        pubsubChannels,
        keyCount,
        role,
      };
    } catch (err: any) {
      return {
        status: "error",
        isHealthy: false,
        latencyMs: Date.now() - start,
      };
    }
  }

  /**
   * Safe Getter with fallback
   */
  public static async get(key: string): Promise<string | null> {
    if (this.isReady()) {
      try {
        return await this.getClient().get(key);
      } catch (err) {
        logger.debug(`[RedisManager] get fallback: ${err}`);
      }
    }
    const item = this.memoryFallback.get(key);
    if (!item) return null;
    if (item.expiresAt && Date.now() > item.expiresAt) {
      this.memoryFallback.delete(key);
      return null;
    }
    return item.value;
  }

  /**
   * Safe Setter with optional TTL in seconds
   */
  public static async set(key: string, value: string, ttlSeconds?: number): Promise<boolean> {
    if (this.isReady()) {
      try {
        if (ttlSeconds && ttlSeconds > 0) {
          await this.getClient().set(key, value, "EX", ttlSeconds);
        } else {
          await this.getClient().set(key, value);
        }
        return true;
      } catch (err) {
        logger.debug(`[RedisManager] set fallback: ${err}`);
      }
    }
    this.memoryFallback.set(key, {
      value,
      expiresAt: ttlSeconds ? Date.now() + ttlSeconds * 1000 : undefined,
    });
    return true;
  }

  /**
   * Safe Deletion
   */
  public static async del(...keys: string[]): Promise<number> {
    if (keys.length === 0) return 0;
    if (this.isReady()) {
      try {
        return await this.getClient().del(...keys);
      } catch (err) {
        logger.debug(`[RedisManager] del fallback: ${err}`);
      }
    }
    let count = 0;
    for (const k of keys) {
      if (this.memoryFallback.delete(k)) count++;
    }
    return count;
  }

  /**
   * Graceful disconnection on shutdown
   */
  public static async close(): Promise<void> {
    logger.info("[RedisManager] Closing Redis connections...");
    const clients = [this.instance, this.publisherInstance, this.subscriberInstance].filter(Boolean) as Redis[];
    await Promise.all(
      clients.map((c) =>
        c.quit().catch(() => {
          c.disconnect();
        })
      )
    );
    this.isConnected = false;
    logger.info("[RedisManager] Redis connections successfully closed.");
  }
}
