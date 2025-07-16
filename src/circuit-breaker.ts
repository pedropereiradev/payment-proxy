import { redis } from "bun";

interface CircuitState {
  state: "CLOSED" | "OPEN" | "HALF_OPEN";
  failures: number;
  lastFailure: number | null;
}

type Processor = "default" | "fallback";

const FAILURE_THRESHOLD = 1;
const RECOVERY_TIMEOUT = 6000;

async function getCircuitState(processor: Processor): Promise<CircuitState> {
  const key = `circuit:${processor}`;

  const data = await redis.hmget(key, ["state", "failures", "lastFailure"]);

  if (!data[0]) {
    await redis.hmset(key, [
      "state",
      "CLOSED",
      "failures",
      "0",
      "lastFailure",
      "",
    ]);

    return {
      state: "CLOSED",
      failures: 0,
      lastFailure: null,
    };
  }

  const [state, failures, lastFailure] = data;

  return {
    state: state as CircuitState["state"],
    failures: parseInt(failures || "0", 10),
    lastFailure:
      lastFailure && lastFailure !== "" ? parseInt(lastFailure, 10) : null,
  };
}

async function setCircuitState(
  processor: Processor,
  circuitState: CircuitState,
): Promise<void> {
  const key = `circuit:${processor}`;

  await redis.hmset(key, [
    "state",
    circuitState.state,
    "failures",
    circuitState.failures.toString(),
    "lastFailure",
    circuitState.lastFailure ? circuitState.lastFailure.toString() : "",
  ]);
}

export async function recordFailure(processor: Processor): Promise<void> {
  const currentState = await getCircuitState(processor);

  currentState.failures += 1;
  currentState.lastFailure = Date.now();

  if (currentState.failures > FAILURE_THRESHOLD) {
    currentState.state = "OPEN";
  }

  await setCircuitState(processor, currentState);
}

export async function recordSuccess(processor: Processor): Promise<void> {
  const currentState = await getCircuitState(processor);

  currentState.failures = 0;
  currentState.state = "CLOSED";
  currentState.lastFailure = null;

  await setCircuitState(processor, currentState);
}

export async function isCircuitOpen(processor: Processor): Promise<boolean> {
  const currentState = await getCircuitState(processor);

  if (currentState.state === "OPEN") {
    if (
      currentState.lastFailure &&
      Date.now() - currentState.lastFailure > RECOVERY_TIMEOUT
    ) {
      currentState.state = "HALF_OPEN";
      await setCircuitState(processor, currentState);

      return false;
    }
    return true;
  }

  return false;
}
