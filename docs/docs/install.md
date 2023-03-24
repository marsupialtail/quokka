# **Installation**

If you plan on trying out Quokka for whatever reason, I'd love to hear from you. Please send an email to zihengw@stanford.edu or join the [Discord](https://discord.gg/6ujVV9HAg3).

Quokka can be installed as a pip package: 
~~~bash
pip3 install pyquokka
~~~

You should also install the latest version of Redis if you are using Quokka locally: 

~~~bash
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

sudo apt-get update
sudo apt-get install redis
~~~

**Quokka is currently tested to work on x86 Linux/Mac environments.**

If you only plan on running Quokka locally, you are done. Here is a [10 min lesson](simple.md) on how it works.

If you are planning on reading files from S3, you need to install the awscli and have your credentials set up.

If you plan on using Quokka for cloud by launching EC2 clusters, there's a bit more setup that needs to be done. Currently Quokka only provides support for AWS. Quokka provides a utility library under `pyquokka.utils` which allows you to manager clusters and connect to them. It assumes that awscli is configured locally and you have a keypair and a security group with the proper configurations. To set these things up, you can follow the [AWS guide](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-quickstart.html). 

Alternatively, if you already have a Ray cluster setup, Quokka can connect to it. It will need to install a couple more things on the Ray cluster (pyquokka and Redis), but it will not launch any new nodes.

More detailed instructions can be found in [Setting Up Cloud Cluster](cloud.md).