# quokka

The plan:

We are either going to use Redis or Memcached to implement the communication and storage layer. I am writing a barebones embedded distributed key-value store in C++ that will be nice to replace these systems in the future but should not count on this. 

Either way, there will be another Python program running alongside the Redis/Memcached server on the reducer, implementing the reducer logic and interfacing with the Redis/Memcached server with a client library. Although everything should eventually be event driven (if using my embedded K-V store, they definitely have to be), currently it doesn't have to be. In fact we can probably just do this.

while True:\n
   redis.sub_channel.get_all_messages()\n
   if new_message():\n
      things_to_send = execute_reducer()
      remove_message_from_mailbox()
      update_state()
      for thing in things_to_send:
        send(thing)

The code to set up a Redis/Memcached server should be common infrastructure. Perhaps there should also be a common library on top of the async Redis client that polls messages and trigger some user defined reducer function. The user defined reducer function can be some arbitrary Python file. 

The argument for Redis is that we 
