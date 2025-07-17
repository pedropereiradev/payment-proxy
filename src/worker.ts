import { redis } from "bun";
import { isCircuitOpen, recordFailure, recordSuccess } from "./circuit-breaker";
import { getHealth } from "./health-check";

const defaultProcessorUrl = Bun.env.DEFAULT_PROCESSOR_URL;
const fallbackProcessorUrl = Bun.env.FALLBACK_PROCESSOR_URL;

async function processPaymentFromQueue(payment: {
  correlationId: string;
  amount: number;
}) {
  try {
    const requestedAt = new Date().toISOString();

    const [defaultHealthy, defaultCircuitOpen] = await Promise.all([
      getHealth("default"),
      isCircuitOpen("default"),
    ]);

    const processorToTry =
      defaultHealthy && !defaultCircuitOpen ? "default" : "fallback";
    const smartTimeout = Math.min(defaultHealthy.minResponseTime * 1.5, 500);

    const response = await fetch(
      `${processorToTry === "default" ? defaultProcessorUrl : fallbackProcessorUrl}/payments`,
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
        signal: AbortSignal.timeout(smartTimeout),
      },
    );

    if (response.ok) {
      await Promise.all([
        recordSuccess(processorToTry),
        redis.hmset(`payment:${payment.correlationId}`, [
          "amount",
          payment.amount.toString(),
          "requestedAt",
          requestedAt,
          "processor",
          processorToTry,
        ]),
      ]);
      return;
    }

    await recordFailure(processorToTry);

    const fallbackProcessor =
      processorToTry === "default" ? "fallback" : "default";
    const fallbackHealthy = await getHealth(fallbackProcessor);
    const smartTimeoutFallback = Math.min(
      fallbackHealthy.minResponseTime * 1.5,
      500,
    );

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
        signal: AbortSignal.timeout(smartTimeoutFallback),
      },
    );

    if (fallbackResponse.ok) {
      await Promise.all([
        recordSuccess(fallbackProcessor),
        redis.hmset(`payment:${payment.correlationId}`, [
          "amount",
          payment.amount.toString(),
          "requestedAt",
          requestedAt,
          "processor",
          fallbackProcessor,
        ]),
      ]);
    } else {
      await recordFailure(fallbackProcessor);
    }
  } catch (_error) {}
}

export async function processPaymentQueue() {
  while (true) {
    try {
      const result = await redis.lpop("payment_queue");

      if (result) {
        const payment = JSON.parse(result);

        processPaymentFromQueue(payment);
      } else {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    } catch (_error) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }
}
