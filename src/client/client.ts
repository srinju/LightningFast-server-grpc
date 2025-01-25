import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

async function runClient() {
  // Load proto definition
  const packageDefinition = protoLoader.loadSync(
    path.join(__dirname, '../src/a.proto'),
    {
      keepCase: true,
      longs: String,
      enums: String,
      defaults: true,
      oneofs: true,
    }
  );
  const proto = grpc.loadPackageDefinition(packageDefinition);
  const HighPerformanceService = (proto as any).highPerformance.HighPerformanceService;
  
  const client = new HighPerformanceService(
    'localhost:9090', 
    grpc.credentials.createInsecure()
  );

  // Function to send a single request with timing
  const sendRequest = (data: string): Promise<void> => {
    const start = performance.now();
    return new Promise((resolve, reject) => {
      client.process({ data }, (error: any, response: any) => {
        const end = performance.now();
        if (error) {
          console.error(`Request failed: ${error.message}, Time: ${(end - start).toFixed(2)}ms`);
          resolve();
        } else {
          resolve();
        }
      });
    });
  };

  // Send 1 million requests with concurrency control
  const CONCURRENCY = 100;
  const TOTAL_REQUESTS = 1_000_000;

  console.time('Total Execution Time');
  
  const batchStartTimes: number[] = [];
  const batchEndTimes: number[] = [];

  for (let i = 0; i < TOTAL_REQUESTS; i += CONCURRENCY) {
    const batchStart = performance.now();
    batchStartTimes.push(batchStart);

    const batch = Array(Math.min(CONCURRENCY, TOTAL_REQUESTS - i)).fill(0).map((_, j) => 
      sendRequest(`Request ${i + j}`)
    );
    
    await Promise.all(batch);
    
    const batchEnd = performance.now();
    batchEndTimes.push(batchEnd);

    console.log(`Batch ${i/CONCURRENCY + 1}: Processed ${i + batch.length} requests, Batch Time: ${(batchEnd - batchStart).toFixed(2)}ms`);
  }

  console.timeEnd('Total Execution Time');

  // Additional timing analysis
  const totalBatches = batchStartTimes.length;
  const avgBatchTime = batchEndTimes.map((end, i) => end - batchStartTimes[i])
    .reduce((a, b) => a + b, 0) / totalBatches;

  console.log(`Total Batches: ${totalBatches}`);
  console.log(`Average Batch Processing Time: ${avgBatchTime.toFixed(2)}ms`);
}

// Run the client
runClient().catch(console.error);