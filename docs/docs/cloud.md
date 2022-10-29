# Setting up Quokka for EC2

To use Quokka for EC2, you need to (at minimum) have an AWS account with permissions to launch instances and create new security groups. You will definitely run into issues since everybody's AWS setup is a little bit different, so please email: zihengw@stanford.edu or [Discord](https://discord.gg/6ujVV9HAg3). 

Quokka requires a security group that allows inbound and outbound connections to ports 5005 (Flight), 6379 (Ray) and 6800 (Redis) from IP addresses within the cluster. For simplicity, you can just enable all inbound and outbound connections from all IP addresses. The easiest way to make this is to manually create an instance on EC2 through the dashboard, e.g. t2.micro, and manually add rules to the security group EC2 assigns that instance. Then you can either copy that security group to a new group, or keep using that modified security group for Quokka. There must be an automated way to do this in the AWS CLI, but I am too lazy to figure it out. If you want to tell me how to do it, I'll post the steps here and buy you a coffee.

You also need to generate a pem key pair. The easiest way to do this, again, is to start a t2.micro in the console and using the dashboard. Save the pem key somewhere and write down the absolute path.

After you have the security group and you can use the `QuokkaClusterManager` in `pyquokka.utils` to spin up a cluster. The code to do this:

~~~python
from pyquokka.utils import QuokkaClusterManager
manager = QuokkaClusterManager(key_name = YOUR_KEY, key_location = ABSOLUTE_PATH_TO_KEY, security_group= SECURITY_GROUP_ID)
cluster = manager.create_cluster(aws_access_key, aws_access_id, num_instances = 4, instance_type = "i3.2xlarge", requirements = ["pytorch"])
cluster.to_json("config.json")
~~~

This would spin up four `i3.2xlarge` instances and install pytorch on each of them. The `QuokkaClusterManager` also has other utilities such as `launch_all`, `terminate_cluster` and `get_cluster_from_json`. Importantly, currently only on-demand instances are supported. This will change in the near future. The most interesting utility is probably `manager.launch_all(command)`, which basically runs a custom command on each machine. You can use this command to massage your cluster into your desired state. In general, all of the machines in your cluster must have all the Python packages you need installed with `pip`.

Importantly, if you are using on demand instances, creating a cluster only needs to happen once. Once you have saved the cluster configs to a json, the next time you want to run a job and use this cluster, you can just do: 

~~~python
from pyquokka.utils import QuokkaClusterManager
manager = QuokkaClusterManager(key_name = YOUR_KEY, key_location = ABSOLUTE_PATH_TO_KEY, security_group= SECURITY_GROUP_ID)
cluster = manager.get_cluster_from_json("config.json")
~~~

This will work if the cluster is either fully stopped or fully running, i.e. every machine must be in either stopped or running state. If the cluster is running, this assumes it was started by running the `get_cluster_from_json` command! **Please do not manually start the instances and try to use `get_cluster_from_json` to connect to a cluster.**

Quokka also plans to extend support to Docker/Kubernetes based deployments based on KubeRay. (Contributions welcome!) Of course, there are plans to support GCP and Azure. The best way to make sure that happens is by sending me a message on email or [Discord](https://discord.gg/YKbK2TVk). 
