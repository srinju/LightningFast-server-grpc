import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';
import { performance } from 'perf_hooks';

interface Metrics {
    totalRequests: number;
    successRequests: number;
    failedRequests: number;
    totalLatency: number;
    currentRPS: number;
    peakRPS: number;
}

interface Config {
    target: string;
    numRequests: number;
    concurrency: number;
    reportDelay: number;
    connections: number;
}

class GRPCLoadTester {
    private config: Config;
    private metrics: Metrics;
    private packageDefinition: any;
    private proto: any;

    constructor(config?: Partial<Config>) {
        this.config = {
            target: 'localhost:9090',
            numRequests: 1_000_000,
            concurrency: 20_000,
            reportDelay: 1000,
            connections: 10,
            ...config
        };

        this.metrics = {
            totalRequests: 0,
            successRequests: 0,
            failedRequests: 0,
            totalLatency: 0,
            currentRPS: 0,
            peakRPS: 0
        };

        this.packageDefinition = protoLoader.loadSync(
            path.join(__dirname, '../src/a.proto'),
            {
                keepCase: true,
                longs: String,
                enums: String,
                defaults: true,
                oneofs: true
            }
        );

        this.proto = grpc.loadPackageDefinition(this.packageDefinition).highPerformance;
    }

    private createConnections(): any[] {
        const connections: any[] = [];
        for (let i = 0; i < this.config.connections; i++) {
            const conn = new this.proto.HighPerformanceService(
                this.config.target,
                grpc.credentials.createInsecure()
            );
            connections.push(conn);
        }
        return connections;
    }

    private async runWorker(
        client: any, 
        requestsPerWorker: number
    ): Promise<void> {
        const request = { data: 'test' };

        for (let i = 0; i < requestsPerWorker; i++) {
            const startTime = performance.now();
            try {
                await new Promise((resolve, reject) => {
                    client.process(request, (error: any, response: any) => {
                        if (error) {
                            this.metrics.failedRequests++;
                            reject(error);
                        } else {
                            this.metrics.successRequests++;
                            resolve(response);
                        }
                    });
                });
            } catch (error) {
                // Error already handled
            } finally {
                const latency = performance.now() - startTime;
                this.metrics.totalRequests++;
                this.metrics.totalLatency += latency * 1000; // Convert to microseconds
            }
        }
    }

    private startReportingMetrics(): () => void {
        const reportInterval = setInterval(() => {
            const currentRPS = this.metrics.totalRequests / (this.config.reportDelay / 1000);
            this.metrics.currentRPS = currentRPS;
            this.metrics.peakRPS = Math.max(this.metrics.peakRPS, currentRPS);

            console.log(`Current RPS: ${currentRPS.toFixed(2)}, ` +
                `Total Requests: ${this.metrics.totalRequests}, ` +
                `Successful: ${this.metrics.successRequests}, ` +
                `Failed: ${this.metrics.failedRequests}`);
        }, this.config.reportDelay);

        return () => clearInterval(reportInterval);
    }

    async runLoadTest(): Promise<void> {
        console.log('Starting load test with configuration:');
        console.log(JSON.stringify(this.config, null, 2));

        const connections = this.createConnections();
        const requestsPerWorker = Math.floor(this.config.numRequests / this.config.concurrency);

        const stopReporting = this.startReportingMetrics();

        const startTime = performance.now();
        const workers = [];

        for (let i = 0; i < this.config.concurrency; i++) {
            const connection = connections[i % this.config.connections];
            workers.push(this.runWorker(connection, requestsPerWorker));
        }

        await Promise.all(workers);
        stopReporting();

        const duration = (performance.now() - startTime) / 1000;
        const finalRPS = this.metrics.totalRequests / duration;
        const avgLatency = this.metrics.totalLatency / this.metrics.totalRequests;

        console.log('\nTest completed');
        console.log(`Duration: ${duration.toFixed(2)} seconds`);
        console.log(`Total requests: ${this.metrics.totalRequests}`);
        console.log(`Successful requests: ${this.metrics.successRequests}`);
        console.log(`Failed requests: ${this.metrics.failedRequests}`);
        console.log(`Average RPS: ${finalRPS.toFixed(2)}`);
        console.log(`Peak RPS: ${this.metrics.peakRPS.toFixed(2)}`);
        console.log(`Average latency: ${(avgLatency / 1000).toFixed(2)} ms`);
    }
}

async function main() {
    const loadTester = new GRPCLoadTester({
        // Optional: Override default configuration
        // numRequests: 500_000,
        // concurrency: 512
    });

    await loadTester.runLoadTest();
}

main().catch(console.error);

export default GRPCLoadTester;