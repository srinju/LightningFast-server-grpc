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
const BATCH_SIZE = 5000; // Process requests in batches of 5000
const NUM_WORKERS = os.cpus().length; // Number of CPU cores as workers
const BATCH_TIMEOUT_MS = 500; // Batch processing timeout in milliseconds
const MAX_QUEUE_SIZE = 500000; // Maximum size of the queue
let totalProcessed = 0;

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
    if (msg === "ready") {
      console.log(`Worker ${worker.process.pid} is ready.`);
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

      // Respond to the client
      callback(null, { status: "ok" });
    },

    streamProcess: (call: any) => {
      console.log("Received streaming request.");
      call.on("data", (request: any) => {
        if (requestQueue.length < MAX_QUEUE_SIZE) {
          requestQueue.push(request);
        } else {
          console.warn("Queue is full. Dropping incoming streaming requests!");
        }
      });

      call.on("end", () => {
        console.log("Stream ended.");
        call.end();
      });
    },
  };

  // Batch processing logic
  setInterval(() => {
    if (requestQueue.length === 0) {
      return;
    }

    // Create a batch of requests
    const batch = requestQueue.splice(0, BATCH_SIZE);

    // Assign a worker to process the batch
    //@ts-ignore
    const worker = cluster.workers[Object.keys(cluster.workers)[0]];
    worker?.send(batch);
    worker?.once("message", (processed: number) => {
      totalProcessed += processed;
      console.log(`Processed ${processed} requests. Total processed: ${totalProcessed}`);
    });
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

  // Graceful shutdown
  process.on("SIGINT", async () => {
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
  });
} else {
  // Worker process logic
  process.send?.("ready");

  process.on("message", async (batch: { data: string }[]) => {
    const processed = await processBatch(batch);
    process.send?.(processed); // Notify the parent process with the count of processed requests
  });

  async function processBatch(batch: { data: string }[]) {
    // Insert requests into the database
    const processedRequests = await prisma.request.createMany({
      data: batch.map((request) => ({
        requestData: request.data,
        timestamp: Date.now(),
      })),
    });

    return processedRequests.count; // Return the count of processed requests
  }
}
