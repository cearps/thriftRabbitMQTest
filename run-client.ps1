# Run Service A (Client) - Windows PowerShell

Set-Location service-a
mvn exec:java '-Dexec.mainClass=com.example.thrift.client.ServiceAMain'
