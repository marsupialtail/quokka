# **Setting up Quokka for EC2**

## The easy way

To use Quokka for EC2, you need to *at minimum* have an AWS account with permissions to launch instances. You will probably run into issues since everybody's AWS setup is a little bit different, so please email: zihengw@stanford.edu or [Discord](https://discord.gg/6ujVV9HAg3). 

First if you haven't already, you must run `aws configure` on your local machine, i.e. the machine you are using to spin up a Quokka cluster and submit jobs to the cluster.

Quokka requires a security group that allows inbound and outbound connections for Arrow Flight, Ray, Redis and SSH. For simplicity, you can just enable all inbound and outbound connections from all IP addresses. You can make a security group like this: 

~~~bash
#!/bin/bash

# Set the name and description of the security group
GROUP_NAME=random-group
GROUP_DESCRIPTION="Custom security group for Quokka"

# Create the security group
aws ec2 create-security-group --group-name "$GROUP_NAME" --description "$GROUP_DESCRIPTION"

# Get the ID of the security group
GROUP_ID=$(aws ec2 describe-security-groups --group-names "$GROUP_NAME" --query "SecurityGroups[0].GroupId" --output text)
echo $GROUP_ID # write this down!

# Allow some inbound TCP traffic
aws ec2 authorize-security-group-ingress --group-id "$GROUP_ID" --protocol tcp --port 0-65535 --cidr 0.0.0.0/0

# Allow all outbound TCP traffic
aws ec2 authorize-security-group-egress --group-id "$GROUP_ID" --protocol tcp --port 0-65535 --cidr 0.0.0.0/0
~~~

You also need to generate a pem key pair. The easiest way to do this is if you don't already have one, is to start a t2.micro instance in the AWS console and create a new keypair. Remember the name of the key pair and where it lives on your computer.

After you have the security group ID (sg-XXXXXX), you can use the `QuokkaClusterManager` in `pyquokka.utils` to spin up a cluster. You can optionally specify an AMI_ID as the base AMI for the machines in the cluster. The first requirement is that it's running Ubuntu. The **second** requirement for this AMI is that the Python version on it must match the Python version on your laptop, or whatever machine that will be running Quokka code and submitting jobs to this cluster. If you don't specify anything, the default will be AWS Ubuntu Server 20.04 AMI, which assume you have Python3.8. 

Now you can spin up a cluster with four lines of code:

~~~python
from pyquokka.utils import QuokkaClusterManager
manager = QuokkaClusterManager(key_name = YOUR_KEY, key_location = ABSOLUTE_PATH_TO_KEY, security_group= SECURITY_GROUP_ID)
cluster = manager.create_cluster(aws_access_secret_key, aws_access_id, num_instances = 4, instance_type = "i3.2xlarge", ami=AMI_ID, requirements = ["sklearn"])
cluster.to_json("config.json")
~~~

This would spin up four `i3.2xlarge` instances and install Sklearn on each of them. This takes around three minutes for me.

The `QuokkaClusterManager` also has other utilities such as `launch_all`, `terminate_cluster` and `get_cluster_from_json`. Importantly, currently only on-demand instances are supported. This will change in the near future. The most interesting utility is probably `manager.launch_all(command)`, which basically runs a custom command on each machine. You can use this command to massage your cluster into your desired state. In general, all of the machines in your cluster must have all the Python packages you need installed with `pip`.

Importantly, if you are using on demand instances, creating a cluster only needs to happen once. Once you have saved the cluster configs to a json, the next time you want to run a job and use this cluster, you can just do: 

~~~python
from pyquokka.utils import QuokkaClusterManager
manager = QuokkaClusterManager(key_name = YOUR_KEY, key_location = ABSOLUTE_PATH_TO_KEY, security_group= SECURITY_GROUP_ID)
cluster = manager.get_cluster_from_json("config.json")
~~~

This will work if the cluster is either fully stopped or fully running, i.e. every machine must be in either stopped or running state. If the cluster is running, this assumes it was started by running the `get_cluster_from_json` command! **Please do not manually start the instances and try to use `get_cluster_from_json` to connect to a cluster.**

Quokka also plans to extend support to Docker/Kubernetes based deployments based on KubeRay. (Contributions welcome!) Of course, there are plans to support GCP and Azure. The best way to make sure that happens is by sending me a message on email or [Discord](https://discord.gg/YKbK2TVk). 

## The hard way

Of course, you might wonder if you can set up the cluster yourself without using `pyquokka.utils`. Indeed you might not trust my setup -- am I stealing your data? Apart from reassuring you that I have little interest in your data, you can also try to manually setup the cluster yourself.

Well it shouldn't be so hard to do this. These are the steps you have to follow: 

* I assume you have your own security group and AMI image that abide by the requirements listed above. Feel free to open only specific ports, but Quokka might not work if you do. A telling sign there's a firewall problem is if Quokka hangs at launch.
* Please do make sure the Python version across the cluster is the same as the Python version on your laptop or whatever machine that will be submitting jobs to this cluster. 
* Now launch a Ray cluster with the security group and AMI image. It's quite simple. Just install Ray on each machine in the cluster, and run `ray start` on the machine you choose to be the master. Now it will spit out a command you should run on the remaining machines. Go run that command on each remaining machine to setup the workers. **Important: the Ray version across the cluster must also match the Ray version on your laptop.**
* You must install Redis server on the master machine. 
~~~bash
curl -fsSL https://packages.redis.io/gpg | sudo gpg --dearmor -o /usr/share/keyrings/redis-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/redis-archive-keyring.gpg] https://packages.redis.io/deb $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/redis.list

sudo apt-get update
sudo apt-get install redis
~~~
* Now `pip install pyquokka` on all the machines in your cluster. That should automatically install all the dependencies you need. Of course if your workflow requires other packages like `PyTorch`, you need to pip install those as well.
* Now start the Redis server on the master. Quokka requires some interesting configs for this, which is in Quokka's github repo under pyquokka folder, redis.conf. Copy that file onto the master and start the Redis server: `redis-server redis.conf --port 6800 --protected-mode no&` 
* You also need to copy the Flight Server file to each worker machine. You can find flight.py in pyquokka folder and copy that to each worker machine in the home directory.
* Now run `aws configure` on each of the machines across the cluster assuming you will be reading data from S3.
* If your instance comes with NVME SSDs, mount the NVME SSD onto `/data`: `sudo mkfs.ext4 -E nodiscard /dev/nvme0n1; sudo mount /dev/nvme0n1 /data;` Otherwise just make a directory called `/data`. Give read and write privleges to this folder: `sudo chmod -R a+rw /data/`.
* You should be done. You should now make a json file that describes the cluster:
~~~json
{"instance_ids": {"0": "i-0cb3e260d80acf9e1", "1": "i-02f307f57aa9217aa", "2": "i-09a5a93e7726b2eee", "3": "i-0af7018eba5c4bf17"}, "cpu_count_per_instance": 8}
~~~

Now you can use this Json file to get a Quokka cluster as described above. 

The hard way is not expected to work right away. In fact I would expect it not: definitely send me a message on [Discord](https://discord.gg/6ujVV9HAg3) or email me if this doesn't work. 