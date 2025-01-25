

//high performance grpc server>>
//which can handle 1 m requests without any problem

//working>

//client sends the request to the process of the grpc server
//requests are added to the queue in form of batch 5000 requests per batch
//periodically in 500ms the batches are sent to the worker threads for processing 
//worker threads do the necessary things and put the data in the database and notify the main thread when done.
//the server continues processing new requests ....

import { PrismaClient } from "@prisma/client";
import * as protoLoader from '@grpc/proto-loader';
import path from "path";
import * as grpc from '@grpc/grpc-js';
import os from 'os';
import cluster from "cluster";

const prisma = new PrismaClient();

//load the proto file>

const packageDefinition = protoLoader.loadSync(path.join(__dirname , '../src/a.proto'));

//put it into a grpc function >

const highPerformanceProto = grpc.loadPackageDefinition(packageDefinition).highPerformance as any; //as we defined a package called highPerformance in the proto file which is nothing but it creates a namespace for accessing the services in the grpc package

//batch settings >>

//batch size is 5000 requests that is hitting our server
//there should be a batch timeout that is sets the delay that sets a delay before processing the request , so that there is no overlapping of request
//and there should be also a queue that is there to hold the requests that are yet to be processed

//The worker thread performs the heavy lifting of processing batches, and the main thread (or parent thread) can track the progress or update its state based on information from the worker thread.

const BATCH_SIZE = 5000; //5000 requests batch size
const NUM_WORKERS = os.cpus().length; //number of worker threads
const BATCH_TIMEOUT_MS = 500; //500 ms batch timeout before processing a batch
const MAX_QUEUE_SIZE = 500000; //max queuue size
let totalProcessed = 0;
//make a request quueue >
let requestQueue : {
    data : string
}[] = [] ;

//worker thread implementaion >
//if main thread then we have to spawn the workers>>

if(cluster.isPrimary){
    
    //fork the workers if that is the primary cluseter
    for(let i = 0 ; i < NUM_WORKERS ; i++){
        cluster.fork();
    }

    //listen for messages from workers >
    cluster.on('message' , (worker , msg) => {
        if(msg == 'ready'){
            console.log(`worker ${worker.process.pid} is ready`);
        }
    });

    //grpc server implementation >>

    const highPerformanceService  = (call : any,callback : any) => {

        const request = call.request;

        //add to queue >
        if(requestQueue.length < MAX_QUEUE_SIZE){ //if the queue is not full
            requestQueue.push(request); //push the request in the queue
        } else {
            console.warn('the queue is full currently , dropping the incoming requests!!');
        }

        //return the resonse >
        callback(null , {
            status : "ok"
        });

    }
    //now from the queue the requests are taken with batch size>>
    //batch processing logic >>
    setInterval(() => {
        if(requestQueue.length == 0){
            return;
        }

        //split requests into batches >
        const batch = requestQueue.splice(0 , BATCH_SIZE);

        //assign a worker to handle the batch >>
        //@ts-ignore
        const worker = cluster.workers[Object.keys(cluster.workers)[0]]; //select any woker
        //send the batch to the worker >
        worker?.send(batch);
        worker?.once('message' , (processed : number) => {
            totalProcessed += processed;
            console.log(`processed ${processed} requests . Total processed requests : ${totalProcessed} `);
        });

    },BATCH_TIMEOUT_MS);

    //start the grpc server>

    const server = new grpc.Server();
    //@ts-ignore
    server.addService(proto.polarmeet.HighPerformanceService.service , highPerformanceService);
    const PORT = 9090;

    server.bindAsync(`0.0.0.0:${PORT}` , grpc.ServerCredentials.createInsecure() , (err : Error | null ,port : number) => {
        if(err){
            console.error("failed to start the grpc server : " , err);
            process.exit(1);
        } else {
            console.log(`grpc server is running on port ${PORT}`);
            server.start();
        }
    });

    //graceful shutdown>>

    process.on('SIGINT' , async () => {
        console.log('shutting down server!!');
        await prisma.$disconnect();

        for(const id in cluster.workers){
            cluster.workers[id]?.kill();
        }
        
        server.tryShutdown(err => {
            if(err){
                console.error("error shutting down server " , err);
            } else {
                console.log("server shut down gracefully!!");
            }
            process.exit(0);
        });
    });

} else {
    //the thread is not primary then the workers are processing so we put the request in the database >>
    process.send?.('ready'); //send ready signal to parent process
    process.on('message' , async(batch : {data : string}[] ) => {
        const processed = await processBatch(batch);
        process.send?.(processed); //send back the number of processed items
    });

    async function processBatch( batch : { data : string }[] ) {
        //enter the request data in the database >
        const processedRequests = await prisma.request.createMany({
            data : batch.map(request => ({
                requestData : request.data,
                timestamp : Date.now(),
            })),
        });

        return  processedRequests.count;
    }
}



