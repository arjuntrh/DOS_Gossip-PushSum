# Gossip Protocol for Distributed Systems
* Implemented Gossip Protocol for information propgation in distributed systems using Akka.NET asynchronous actor facility.
* Applied the algorithm on 4 different topologies (Line, 2D, Imperfect 2D, Full Network) to compare algorithm convergence and performance. 
* Achieved 90% convergence and extended the application for aggregate computation using Push-Sum algorithm.

### Largest network used: 
* For Gossip algorithm: 
    * Full network topology: 10000 nodes  
    * Imperfect 2D topology: 10000 nodes 
    * 2D topology: 10000 nodes 
    * Line topology: 10000 nodes 
 
* For Push-Sum algorithm: 
    * Full network topology: 10000 nodes  
    * Imperfect 2D topology: 10000 nodes 
    * 2D topology: 10000 nodes 
    * Line topology: 1000 nodes 
  
### To Run the Application:
* Open a terminal in the project folder and run the command: \
   &nbsp;&nbsp;&nbsp; ```dotnet fsi --langversion:preview .\project2.fsx 100 arg1 arg2 arg3```
   * arg1: Number of nodes
   * arg2: Topology type
   * arg3: Algorithm name \
* For example, to run Gossip algorithm on a 2D network with 100 nodes: \
```dotnet fsi --langversion:preview .\project2.fsx 100 full gossip```
