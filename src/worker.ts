import { redis } from "bun";
import { getHealth } from "./health-check";

let cachedProcessor: { name: string; url: string } | null = null;
let processorCacheExpiry = 0;

async function getCachedProcessor() {
  const now = Date.now();
  if (cachedProcessor && now < processorCacheExpiry) {
    return cachedProcessor;
  }
  
  cachedProcessor = await getHealth();
  processorCacheExpiry = now + 2000;
  return cachedProcessor;
}

const defaultProcessorUrl = Bun.env.DEFAULT_PROCESSOR_URL;
const fallbackProcessorUrl = Bun.env.FALLBACK_PROCESSOR_URL;

async function processPaymentFromQueue(
  payment: {
    correlationId: string;
    amount: number;
  },
  retryCount = 0,
) {
  try {
    const requestedAt = new Date().toISOString();

    const healthyProcessor = await getCachedProcessor();

    const response = await fetch(`${healthyProcessor.url}/payments`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        correlationId: payment.correlationId,
        amount: payment.amount,
        requestedAt,
      }),
    });

    if (response.ok) {
      await redis.hmset(`payment:${payment.correlationId}`, [
        "amount",
        payment.amount.toString(),
        "requestedAt",
        requestedAt,
        "processor",
        healthyProcessor.name,
      ]);

      return;
    }

    const fallbackProcessor =
      healthyProcessor.name === "default" ? "fallback" : "default";

    const fallbackResponse = await fetch(
      `${fallbackProcessor === "default" ? defaultProcessorUrl : fallbackProcessorUrl}/payments`,
      {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          correlationId: payment.correlationId,
          amount: payment.amount,
          requestedAt,
        }),
      },
    );

    if (fallbackResponse.ok) {
      await redis.hmset(`payment:${payment.correlationId}`, [
        "amount",
        payment.amount.toString(),
        "requestedAt",
        requestedAt,
        "processor",
        fallbackProcessor,
      ]);
    }
  } catch (error) {
    const isTimeoutError = error.name === "TimeoutError";

    if (isTimeoutError && retryCount < 3) {
      await new Promise((resolve) => setTimeout(resolve, 50));
      return processPaymentFromQueue(payment, retryCount + 1);
    } else {
      console.log(`${retryCount || 0} Payment processing error:`, error);
    }
  }
}

async function processBatch() {
  try {
    const [result1, result2, result3, result4, result5] = await Promise.all([
      redis.lpop("payment_queue"),
      redis.lpop("payment_queue"),  
      redis.lpop("payment_queue"),
      redis.lpop("payment_queue"),
      redis.lpop("payment_queue")
    ]);
    
    const payments = [result1, result2, result3, result4, result5].filter(Boolean).map(result => JSON.parse(result));
    
    if (payments.length > 0) {
      await Promise.all(payments.map(payment => processPaymentFromQueue(payment)));
      return payments.length;
    }
    
    return 0;
  } catch (_error) {
    return 0;
  }
}

export async function processPaymentQueue() {
  while (true) {
    const processed = await processBatch();
    
    if (processed === 0) {
      await new Promise((resolve) => setTimeout(resolve, 1));
    }
  }
}
