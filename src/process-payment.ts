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
      keys.map((key) =>
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
        summary[processor].totalAmount += parseFloat(amount);
      }
    }
    return summary;
  } catch (_error) {
    throw new Error("Failed to get payment summary");
  }
}
