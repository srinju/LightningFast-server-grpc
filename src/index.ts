

//high performance grpc server>>
//which can handle 1 m requests without any problem

import { PrismaClient } from "@prisma/client";
import * as protoLoader from '@grpc/proto-loader';
import path from "path";
import * as grpc from '@grpc/grpc-js';

const prisma = new PrismaClient();

//load the proto file>

const packageDefinition = protoLoader.loadSync(path.join(__dirname , '../src/a.proto'));

//put it into a grpc function >

const proto = grpc.loadPackageDefinition(packageDefinition);

//batch settings >>

//batch size is 5000 requests that is hitting our server
//there should be a batch timeout that is sets the delay that sets a delay before processing the request , so that there is no overlapping of request
//and there should be also a queue that is there to hold the requests that are yet to be processed

const BATCH_SIZE = 5000;
const BATCH_TIMEOUT_MS = 500; //500 ms batch timeout before processing a batch
let requestQueue : {
    data : string
}[] = [] ;

//batch processor >

setInterval(async () => {

    if(requestQueue.length >  0 ){
        const batch = requestQueue.splice(0,BATCH_SIZE); //take 5000 requests at a time

        try {

            await prisma.request.createMany({
                data : batch.map((req) => ({
                    requestData : req.data,
                    timestamp : Date.now()
                })),
            });
            console.log(`processed ${batch.length} requests`);

        } catch(err) {
            console.error("failed to insert batch " , err);
        }
    }

},BATCH_TIMEOUT_MS);

//define GRPC service >>

const highPerformanceService = {
    Process : async (
        call : any,
        callback : (err : Error | null , response : { status : string}) => void
    ) => {
        const requests = call.request;
        if(requests.length < BATCH_SIZE * 2){
            //add requests to queue if there is space >>
            requestQueue.push(requests);
        } else {
            console.warn("request queue is full");
        }
        callback(null , {
            status : "ok"
        });
    }
};

//start the grpc server>

const server = new grpc.Server();
//@ts-ignore
server.addService(proto.polarmeet.HighPerformanceService.service , highPerformanceService);
const PORT = 9090;

server.bindAsync(`0.0.0.0:${PORT}` , grpc.ServerCredentials.createInsecure() , (err,port) => {
    if(err){
        console.error("failed to start the grpc server : " , err);
        return; 
    } else {
        console.log(`grpc server is running on port ${PORT}`);
        server.start();
    }
});

