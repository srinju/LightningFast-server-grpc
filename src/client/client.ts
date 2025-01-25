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
  //create grpc client 
  const client = new HighPerformanceService(
    'localhost:9090', 
    grpc.credentials.createInsecure()
  );

  // Function to send a single request
  const sendRequest = (data: string): Promise<void> => {
    return new Promise((resolve, reject) => {
      client.process({ data }, (error: any, response: any) => {
        if (error) {
          console.error(`Request failed: ${error.message}`);
          resolve(); // Continue processing other requests
        } else {
          resolve();
        }
      });
    });
  };

  // Send 1 million requests with concurrency control
  const CONCURRENCY = 100; // Adjust based on your system capabilities
  const TOTAL_REQUESTS = 1_000_000;

  console.time('Total Request Time');
  
  for (let i = 0; i < TOTAL_REQUESTS; i += CONCURRENCY) {
    const batch = Array(Math.min(CONCURRENCY, TOTAL_REQUESTS - i)).fill(0).map((_, j) => 
      sendRequest(`Request ${i + j}`)
    );
    
    await Promise.all(batch);
    console.log(`Processed ${i + batch.length} requests`);
  }

  console.timeEnd('Total Request Time');
}

// Run the client
runClient().catch(console.error);