import { redis } from "bun";

export async function processPayment(payment: {
  correlationId: string;
  amount: number;
}) {
  await redis.lpush("payment_queue", JSON.stringify(payment));

  return;
}

export async function getPaymentsSummary(
  from?: string | null,
  to?: string | null,
) {
  try {
    const keys = await redis.send("KEYS", ["payment:*"]);
    const payments = await Promise.all(
      keys.map((key: string) =>
        redis.hmget(key, ["amount", "requestedAt", "processor"]),
      ),
    );

    const summary = {
      default: { totalRequests: 0, totalAmount: 0 },
      fallback: { totalRequests: 0, totalAmount: 0 },
    };
    const filteredPayments = payments.filter(
      ([_amount, requestedAt, _processor]) => {
        if (!from && !to) return true;
        const paymentTime = new Date(requestedAt).getTime();
        const fromTime = from ? new Date(from).getTime() : 0;
        const toTime = to ? new Date(to).getTime() : Infinity;
        return paymentTime >= fromTime && paymentTime <= toTime;
      },
    );
    for (const [amount, _requestedAt, processor] of filteredPayments) {
      if (processor === "default" || processor === "fallback") {
        summary[processor].totalRequests += 1;
        summary[processor].totalAmount += Number(amount);
      }
    }

    return {
      default: {
        totalRequests: summary.default.totalRequests,
        totalAmount: Number(summary.default.totalAmount.toFixed(2)),
      },
      fallback: {
        totalRequests: summary.fallback.totalRequests,
        totalAmount: Number(summary.fallback.totalAmount.toFixed(2)),
      },
    };
  } catch (_error) {
    throw new Error("Failed to get payment summary");
  }
}

export async function purgePayments() {
  const keys = await redis.send("KEYS", ["payment:*"]);
  if (keys.length > 0) {
    await redis.send("DEL", keys);
  }

  await redis.send("DEL", ["payment_queue"]);

  return;
}
