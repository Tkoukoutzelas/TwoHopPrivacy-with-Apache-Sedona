# TwoHopPrivacy-with-Apache-Sedona
This repository holds the implementation for the Bachelor's Thesis of Themistoklis Koukoutzelas. The implementation optimizes the execution time of Two-Hop Privacy-Preserving Nearest Friend Searches(https://link.springer.com/article/10.1007/s10115-018-1313-8)

# Setup
To run the above code, the following need to be installed: <br />
Python: 3.9 Version <br />
Apache Spark: 3.5.4 Version <br />
Apache Sedona : 1.7.0 Version <br />

The executable file is TwoHopSedona.py

Also, the following arguments need to be provided respectively,before execution: NumberOfUsers,KAnonymity,NumberOfPOIs,NumberOfPartitions,NumberOfNearestNeighbors <br />

All the above arguments are integers

# Important note
The file PrivateNearestNeighbors2.py, has been taken from the original implementation. It holds two revised functions, adapted to the new Parallel Implementation in Apache Sedona. <br />
Those functions are new_point_x and new_point_y
