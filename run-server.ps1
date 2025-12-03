# Run Service B (Server) - Windows PowerShell

Set-Location service-b
mvn exec:java '-Dexec.mainClass=com.example.thrift.server.ServiceBMain'
