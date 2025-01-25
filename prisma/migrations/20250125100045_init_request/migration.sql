-- CreateTable
CREATE TABLE "Request" (
    "id" TEXT NOT NULL,
    "requestData" VARCHAR(255) NOT NULL,
    "timestamp" BIGINT NOT NULL,

    CONSTRAINT "Request_pkey" PRIMARY KEY ("id")
);
