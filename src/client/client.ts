import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";

// Function to generate random data for requests
function generateRandomData(): string {
  return Math.random().toString(36).substring(2, 15);
}

async function runLoadTest() {
  // Load the proto file
  const packageDefinition = protoLoader.loadSync(path.join(__dirname, "../src/a.proto"), {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const proto = grpc.loadPackageDefinition(packageDefinition).highPerformance as any;

  // Create gRPC client
  const client = new proto.HighPerformanceService(
    "localhost:9090",
    grpc.credentials.createInsecure()
  );

  // Total number of requests to send
  const TOTAL_REQUESTS = 1_000_000;
  const CONCURRENCY = 100; // Number of concurrent requests

  // Track progress
  let completedRequests = 0;
  let failedRequests = 0;

  // Performance tracking
  const startTime = Date.now();

  // Create a promise-based wrapper for the gRPC call
  const makeRequest = (data: string): Promise<void> => {
    return new Promise((resolve, reject) => {
      client.streamProcess({ data }, (error: Error | null, response: any) => {
        if (error) {
          failedRequests++;
          reject(error);
        } else {
          completedRequests++;
          resolve();
        }
      });
    });
  };

  // Concurrent request function
  const sendConcurrentRequests = async () => {
    const requests: Promise<void>[] = [];

    for (let i = 0; i < CONCURRENCY; i++) {
      requests.push(makeRequest(generateRandomData()));
    }

    await Promise.all(requests);
  };

  // Main load test execution
  try {
    console.log(`Starting load test to send ${TOTAL_REQUESTS} requests...`);

    while (completedRequests < TOTAL_REQUESTS) {
      await sendConcurrentRequests();

      // Optional: Log progress periodically
      if (completedRequests % 10000 === 0) {
        console.log(`Sent ${completedRequests} requests...`);
      }
    }

    // Calculate and log results
    const endTime = Date.now();
    const duration = (endTime - startTime) / 1000; // in seconds
    const requestsPerSecond = completedRequests / duration;

    console.log("\n--- Load Test Results ---");
    console.log(`Total Requests: ${completedRequests}`);
    console.log(`Failed Requests: ${failedRequests}`);
    console.log(`Duration: ${duration.toFixed(2)} seconds`);
    console.log(`Requests per Second: ${requestsPerSecond.toFixed(2)}`);
  } catch (error) {
    console.error("Load test failed:", error);
  } finally {
    // Close the client connection
    client.close();
  }
}

// Execute the load test
runLoadTest();
