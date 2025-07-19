import { redis } from "bun";
import { getHealth } from "./health-check";

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

    const healthyProcessor = await getHealth();

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

export async function processPaymentQueue() {
  while (true) {
    try {
      const result = await redis.lpop("payment_queue");

      if (result) {
        const payment = JSON.parse(result);

        await processPaymentFromQueue(payment);
      } else {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }
    } catch (_error) {
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
  }
}
