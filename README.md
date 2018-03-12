# Regina and The Plastics KV DB

## Requirements
This project is built upon and intended to be run on Golang Ver. 1.9.2

## Running This System
The following is to start up the KV server for tracking network nodes
server.go: `server.go -c config.json`

The following is to start up a network node for the KV network
node.go: `go run node.go [server ip:port]`

## System Overview
The Regina and The Plastics Key-Value Database is a distributed key-value store that is to be hosted on multiple instances of or separate datastores. This solution solves the problem of having a single point of failure and high availablitly. Within the system, one node, known as the coordinator, shall act as master node that hosts all user-input data. All other nodes of the network act as primary replicants.

## System Design

### General Architecture

### Server

### Node

### DKV API

## Handling Failure

## Azure Deployment

## Assumptions and Constraints