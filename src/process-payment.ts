import { fetch, sql } from "bun";
import { isHealthy } from "./health-check";
import { isCircuitOpen, recordFailure, recordSuccess } from "./circuit-breaker";

const defaultProcessorUrl = Bun.env.DEFAULT_PROCESSOR_URL;
const fallbackProcessorUrl = Bun.env.FALLBACK_PROCESSOR_URL;

export async function processPayment(payment: {
  correlationId: string;
  amount: number;
}) {
  try {
    const requestedAt = new Date().toISOString();

    const defaultHealthy = await isHealthy("default");

    const defaultCircuitOpen = await isCircuitOpen("default");

    const processorToTry =
      defaultHealthy && !defaultCircuitOpen ? "default" : "fallback";

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
        signal: AbortSignal.timeout(500),
      },
    );

    if (response.ok) {
      await recordSuccess(processorToTry);
      await sql`
          INSERT INTO payments (correlation_id, amount, requested_at, processor)
          VALUES (${payment.correlationId}, ${payment.amount}, ${requestedAt}, ${processorToTry})
        `;

      return;
    }

    await recordFailure(processorToTry);

    const fallbackProcessor =
      processorToTry === "default" ? "fallback" : "default";

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
        signal: AbortSignal.timeout(500),
      },
    );

    if (fallbackResponse.ok) {
      await recordSuccess(fallbackProcessor);
      await sql`
          INSERT INTO payments (correlation_id, amount, requested_at, processor)
          VALUES (${payment.correlationId}, ${payment.amount}, ${requestedAt}, ${fallbackProcessor})
        `;

      return;
    } else {
      await recordFailure(fallbackProcessor);
      throw new Error("Both processors failed");
    }
  } catch (error) {
    throw new Error("Failed to process payment");
  }
}

export async function getPaymentsSummary(from?: string, to?: string) {
  try {
    let query = sql`SELECT processor, COUNT(*) as "totalRequests", SUM(amount) as "totalAmount" FROM payments WHERE 1=1`;

    if (from) {
      query = sql`${query} AND requested_at >= ${from}`;
    }
    if (to) {
      query = sql`${query} AND requested_at <= ${to}`;
    }
    query = sql`${query} GROUP BY processor`;

    const results = await query;

    const summary = {
      default: { totalRequests: 0, totalAmount: 0 },
      fallback: { totalRequests: 0, totalAmount: 0 },
    };

    results.forEach((result) => {
      summary[result.processor] = {
        totalRequests: parseInt(result.totalRequests) || 0,
        totalAmount: parseFloat(result.totalAmount) || 0,
      };
    });

    return summary;
  } catch (_error) {
    throw new Error("Failed to get payment summary");
  }
}
