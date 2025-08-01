import { redis } from "bun";

const defaultProcessorUrl = Bun.env.DEFAULT_PROCESSOR_URL as string;
const fallbackProcessorUrl = Bun.env.FALLBACK_PROCESSOR_URL as string;

const EXPIRE_TIME = 5;
const INSTANCE_ID = Bun.env.INSTANCE_ID || "api1";
const HEALTH_CHECK_INSTANCE = "api1";

interface HealthCheckResponse {
  failing: boolean;
  minResponseTime: number;
}

interface FetchHealthResponse {
  defaultProcessor: HealthCheckResponse;
  fallbackProcessor: HealthCheckResponse;
}

async function cacheHealthCheck(data: FetchHealthResponse) {
  await redis.set(`health`, JSON.stringify(data));
  await redis.expire(`health`, EXPIRE_TIME);
}

async function getCachedHealth(): Promise<FetchHealthResponse | null> {
  const healthData = await redis.get(`health`);

  if (!healthData) {
    return null;
  }

  return JSON.parse(healthData) as FetchHealthResponse;
}

async function fetchHealthFromProcessors(): Promise<FetchHealthResponse> {
  try {
    const [defaultResponse, fallbackResponse] = await Promise.all([
      fetch(`${defaultProcessorUrl}/payments/service-health`),
      fetch(`${fallbackProcessorUrl}/payments/service-health`),
    ]);

    if (!defaultResponse.ok && !fallbackResponse.ok) {
      throw new Error(`Failed to fetch health from processors`);
    }

    const defaultHealth = await defaultResponse.json();
    const fallbackHealth = await fallbackResponse.json();

    return {
      defaultProcessor: defaultHealth,
      fallbackProcessor: fallbackHealth,
    } as FetchHealthResponse;
  } catch (_error) {
    return {
      defaultProcessor: { failing: true, minResponseTime: 500 },
      fallbackProcessor: { failing: true, minResponseTime: 500 },
    };
  }
}

export async function getHealth(): Promise<{ name: string; url: string }> {
  const cachedData = await getCachedHealth();

  if (cachedData) {
    return chooseProcessor(cachedData);
  }

  if (INSTANCE_ID === HEALTH_CHECK_INSTANCE) {
    const health = await fetchHealthFromProcessors();
    await cacheHealthCheck(health);
    return chooseProcessor(health);
  }

  await new Promise((resolve) => setTimeout(resolve, 10));
  const retryCache = await getCachedHealth();
  if (retryCache) {
    return chooseProcessor(retryCache);
  }

  return { name: "default", url: defaultProcessorUrl };
}

async function chooseProcessor({
  defaultProcessor,
  fallbackProcessor,
}: FetchHealthResponse) {
  if (defaultProcessor.failing && fallbackProcessor.failing) {
    return { name: "default", url: defaultProcessorUrl };
  }

  if (defaultProcessor.failing) {
    return { name: "fallback", url: fallbackProcessorUrl };
  }

  if (fallbackProcessor.failing) {
    return { name: "default", url: defaultProcessorUrl };
  }

  if (
    defaultProcessor.minResponseTime <=
    fallbackProcessor.minResponseTime * 1.5
  ) {
    return { name: "default", url: defaultProcessorUrl };
  }

  return { name: "fallback", url: fallbackProcessorUrl };
}
