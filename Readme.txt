************************ Single source shortest path *****************************

1) The project was created using Eclipse Luna EE. The source files are under package "pkg". 
Hence, while executing the included jar file in Hadoop, we must incldue the package name infront of the jar file.

2) Below is the syntax to submit the job:

	hadoop jar ~/Desktop/ShortestPath.jar pkg.ShortestPathDriver Input Output

   Where, ShortestPath.jar is the name of the jar, Input is the folder name of input directory and Output is the Output directory that will be created on HDFS.

3) After each iteration, a new folder with name Output_Iteration_X will be created and the intermediate output files are stored in this folder.
4) The final output files are stored in the last iteration folder. 
5) A new folder "Node_Summary" will be created to store the node summary. This file contains the list of all node, their adjacency list and the number of links. 
6) The partitioner is coded to dynamically distribute the keys equally among all the reducers. 
