# quokka

The plan:

We are either going to use Redis to implement the communication and storage layer. I am writing a barebones embedded distributed key-value store in C++ that will be nice to replace these systems in the future but should not count on this. 

Either way, there will be another Python program running alongside the Redis server on the reducer, implementing the reducer logic and interfacing with the Redis server with a client library. Although everything should eventually be event driven (if using my embedded K-V store, they definitely have to be), currently it doesn't have to be. In fact we can probably just do this.

For simplicity, each task node will host two Redis servers. One for mailbox and one for its internal state. 
```
while True:  
   redis.atomic_get_del_messages_from_mailbox() 
   if new_message():  
      things_to_send = execute_reducer()   
      update_state()  
      for thing in things_to_send:  
        send(thing)  
```


The code to set up a Redis server should be common infrastructure. Perhaps there should also be a common library on top of the async Redis client that polls messages and trigger some user defined reducer function. The user defined reducer function can be some arbitrary Python file. There needs to be some kind of coordination mechanism 

Both Redis and Memcached would work. Redis is the clear winner because 1) support for atomic transactions 2) doesn't delete stuff by default 3) keyspace alerts, though we might not need it at the moment 4) more flexible data structures that makes implementing mailbox easier.
