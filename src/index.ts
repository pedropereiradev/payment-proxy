import {
  getPaymentsSummary,
  processPayment,
  purgePayments,
} from "./process-payment";
import { processPaymentQueue } from "./worker";

processPaymentQueue();

Bun.serve({
  routes: {
    "/payments": {
      POST: async (req) => {
        const payment = (await req.json()) as {
          correlationId: string;
          amount: number;
        };

        if (!payment.correlationId || !payment.amount) {
          return new Response(null, { status: 400 });
        }

        processPayment(payment);

        return new Response(null, { status: 201 });
      },
    },
    "/payments-summary": {
      GET: async (req: Bun.BunRequest) => {
        const url = new URL(req.url);
        const params = url.searchParams;

        const from = params.get("from");
        const to = params.get("to");

        const summary = await getPaymentsSummary(from, to);

        return Response.json(summary);
      },
    },
    "/purge-payments": {
      POST: async () => {
        await purgePayments();

        return Response.json({ message: "All payments purged." });
      },
    },
    "/health": {
      GET: () => Response.json({ ping: "pong" }),
    },
  },
  error(error) {
    return new Response(`Internal Error: ${error.message}`, {
      status: 500,
      headers: {
        "Content-Type": "text/plain",
      },
    });
  },
});
