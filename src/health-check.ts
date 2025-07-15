import { redis } from "bun";

const defaultProcessorUrl = Bun.env.DEFAULT_PROCESSOR_URL;
const fallbackProcessorUrl = Bun.env.FALLBACK_PROCESSOR_URL;

const EXPIRE_TIME = 5;

type Processor = "default" | "fallback";
interface HealthCheckResponse {
  failing: boolean;
  minResponseTime: number;
}

async function cacheHealthCheck(
  processor: Processor,
  data: HealthCheckResponse,
) {
  await redis.set(`health:${processor}`, JSON.stringify(data));
  await redis.expire(`health:${processor}`, EXPIRE_TIME);
}

async function getCachedHealth(
  processor: Processor,
): Promise<HealthCheckResponse | null> {
  const healthData = await redis.get(`health:${processor}`);

  if (!healthData) {
    return null;
  }

  return JSON.parse(healthData) as HealthCheckResponse;
}

async function fetchHealthFromProcessor(
  processor: Processor,
): Promise<HealthCheckResponse> {
  try {
    const response = await fetch(
      `${processor === "default" ? defaultProcessorUrl : fallbackProcessorUrl}/payments/service-health`,
      { signal: AbortSignal.timeout(500) },
    );

    if (!response.ok) {
      throw new Error(`Failed to fetch health from ${processor} processor`);
    }

    if (response.status === 429) {
      throw new Error("Rate limit exceeded");
    }

    const data = await response.json();

    return data as HealthCheckResponse;
  } catch (_error) {
    return { failing: true, minResponseTime: 500 };
  }
}

export async function getHealth(
  processor: Processor,
): Promise<HealthCheckResponse> {
  const cachedData = await getCachedHealth(processor);

  if (cachedData) {
    return cachedData;
  }

  const health = await fetchHealthFromProcessor(processor);

  await cacheHealthCheck(processor, health);

  return health;
}

export async function isHealthy(processor: Processor): Promise<boolean> {
  const health = await getHealth(processor);

  return !health.failing;
}
