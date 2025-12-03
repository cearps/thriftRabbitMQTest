#!/bin/bash
# Run Service B (Server) - Mac/Linux/Git Bash

cd service-b
mvn exec:java -Dexec.mainClass="com.example.thrift.server.ServiceBMain"
