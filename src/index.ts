import { PrismaClient } from "@prisma/client";
import * as protoLoader from "@grpc/proto-loader";
import path from "path";
import * as grpc from "@grpc/grpc-js";
import os from "os";
import cluster from "cluster";

const prisma = new PrismaClient();

// Load the proto file
const packageDefinition = protoLoader.loadSync(
  path.join(__dirname, "../src/a.proto"),
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
  }
);

const proto = grpc.loadPackageDefinition(packageDefinition) as any;
const HighPerformanceService = proto.highPerformance.HighPerformanceService;

// Batch settings
const BATCH_SIZE = 500_000; // Process requests in batches of 200,000
const NUM_WORKERS = os.cpus().length; // Number of CPU cores as workers
const BATCH_TIMEOUT_MS = 50; // Batch processing timeout in milliseconds
const MAX_QUEUE_SIZE = 1_000_000; // Maximum size of the queue
const MAX_REQUESTS = 1_000_000; // Total requests to process before shutting down

let totalProcessed = 0; // Track total processed requests

// Request queue
let requestQueue: { data: string }[] = [];

// Worker thread implementation
if (cluster.isPrimary) {
  console.log(`Primary process ${process.pid} is running.`);

  // Spawn worker processes
  for (let i = 0; i < NUM_WORKERS; i++) {
    cluster.fork();
  }

  // Listen for messages from worker processes
  cluster.on("message", (worker, msg) => {
    if (msg.type === "ready") {
      console.log(`Worker ${worker.process.pid} is ready.`);
    } else if (msg.type === "processed") {
      totalProcessed += msg.count;

      console.log(`Total processed: ${totalProcessed}/${MAX_REQUESTS}`);

      // Check if we have reached the maximum number of requests
      if (totalProcessed >= MAX_REQUESTS) {
        console.log("Maximum requests processed. Initiating shutdown...");
        shutdown();
      }
    }
  });

  // gRPC service implementation
  const highPerformanceService = {
    process: (call: any, callback: any) => {
      const request = call.request;

      // Add to the queue
      if (requestQueue.length < MAX_QUEUE_SIZE) {
        requestQueue.push(request); // Push the request to the queue
      } else {
        console.warn("Queue is full. Dropping incoming requests!");
      }

      callback(null, { status: "ok" });
    },
  };

  // Batch processing logic
  setInterval(() => {
    if (requestQueue.length === 0) {
      return;
    }

    const batch = requestQueue.splice(0, BATCH_SIZE);

    for (const workerId in cluster.workers) {
      const worker = cluster.workers[workerId];
      if (worker) {
        worker.send(batch);
      }
    }

    console.log(`Batch sent for processing: ${batch.length} requests`);
  }, BATCH_TIMEOUT_MS);

  // Start the gRPC server
  const server = new grpc.Server();
  server.addService(HighPerformanceService.service, highPerformanceService);

  const PORT = 9090;
  server.bindAsync(`0.0.0.0:${PORT}`, grpc.ServerCredentials.createInsecure(), (err, port) => {
    if (err) {
      console.error("Failed to bind server:", err);
      process.exit(1);
    } else {
      console.log(`gRPC server is running on port ${port}`);
      server.start();
    }
  });

  // Graceful shutdown logic
  async function shutdown() {
    console.log("Shutting down server...");

    await prisma.$disconnect();

    for (const id in cluster.workers) {
      cluster.workers[id]?.kill();
    }

    server.tryShutdown((err) => {
      if (err) {
        console.error("Error shutting down server:", err);
      } else {
        console.log("Server shut down gracefully!");
      }
      process.exit(0);
    });
  }

  process.on("SIGINT", shutdown);
} else {
  // Worker process logic
  process.send?.({ type: "ready" });

  process.on("message", async (batch: { data: string }[]) => {
    const processed = await processBatch(batch);
    process.send?.({ type: "processed", count: processed });
  });

  async function processBatch(batch: { data: string }[]) {
    const result = await prisma.request.createMany({
      data: batch.map((request) => ({
        requestData: request.data,
        timestamp: Date.now(),
      })),
      skipDuplicates: true, // Skip duplicate entries in the database
    });

    return result.count; // Return the count of processed requests
  }
}
