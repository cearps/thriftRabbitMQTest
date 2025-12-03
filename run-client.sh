#!/bin/bash
# Run Service A (Client) - Mac/Linux/Git Bash

cd service-a
mvn exec:java -Dexec.mainClass="com.example.thrift.client.ServiceAMain"
