# NextLevelDB Server

NextLevelDB is a relational database that runs on top of a Key-Value-Store, LevelDB. The key in the lower KVS layer comprises of a table key prefix then the primary key of the relational record.

`nextleveldb-server` contains the LevelDB layer, a processing layer, and a communication layer which communicates with clients over websockets.
