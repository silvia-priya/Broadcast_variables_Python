# Broadcast_variables_Python

The broadcast variables are read only shared variables that are cached and available on all nodes in a cluster in order to access or use by tasks.
Rather than sending this variable to every node along with the data spark distributes these variables to the machines using efficient broadcast algorithms 
to reduce data traffic and costs.
These variables are serialized in the cache and deserialized before accessed.
