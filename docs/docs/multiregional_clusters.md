# Spinning up Multiregional AWS Clusters using Quokka

## How to connect instances from multiple regions in Quokka

Quokka now provides an interface which allows users to spin up a connected cluster of AWS EC2 instances across multiple regions. To do so, we first need to create a config.yaml file that specifies the details of the instances that will be included in the cluster. This config.yaml file needs to be set up according to the specifications outlined [here](https://docs.ray.io/en/latest/cluster/vms/references/ray-cluster-configuration.html). In order to spin up a certain number of workers in addition to the head node, the min_workers should be set to the desired number. Make sure that the specified number of max_workers is greater than the specified number of min_workers.

In order to spin up instances in different regions, we need to create a new config file for each region in which we want to launch instances. Once we have our config files, we call ```ray up /path/to/config_region1.yaml``` from the command line. This will spin up the instances in region1. To spin up instances in other regions, we need to call ```ray up /path/to/config_regionX.yaml``` on each regional config file respectively.

Once all instances are up and running, we can then create a QuokkaClusterManager and subsequently call the `get_multiple_clusters_from_yaml` method. Here is an example of how to do this:

```
from pyquokka.utils import *
manager = QuokkaClusterManager(key_name=”my_key”, key_location=”/path/to/key”, security_group=”abc”)
cluster_list = ["config_region1.yaml", "config_region2.yaml"]
results = manager.get_multiple_clusters_from_yaml(cluster_list, aws_access_key, aws_access_id, requirements = ["numpy", "pandas"], spill_dir = "/data")

from pyquokka.df import QuokkaContext
qc = QuokkaContext(results[0])
``` 

It is important to note that `results` is a tuple of values: an instance of a Quokka EC2Cluster class at index 0 and a dictionary containing more regional information about the instances in the cluster at index 1.


## Important Information about Multiregional Clusters


### AMIs

AWS AMIs are region specific. Therefore, you should use region-appropriate AMIs for the instances that you are spinning up in each region. If you have AMIs in one region that you want to copy to another region, you can do as described in this [post](https://medium.com/@jayantspeaks/introduction-to-amazon-machine-instance-ami-1ccec45eab9d#:~:text=Copying%20an%20AMI%20%3A-,AMIs%20are%20region%20specific.,and%20select%20the%20destination%20region).


### VPC Peering for Data Transfer

In order to spin up a multi-region cluster, Quokka requires the VPCs of all involved instances to be peered. The idea of VPC peering is explained in more detail [here](https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-basics.html). 

It is easiest to create a VPC peering connection from the AWS console. The steps to establish a VPC peering connection are described in the AWS docs [here](https://docs.aws.amazon.com/vpc/latest/peering/create-vpc-peering-connection.html). 

Since we are talking about setting up a multiregional cluster here, you most likely will want to peer two VPC on the same account, but for different regions. An in-depth tutorial on how to do this can be found [here](https://docs.aws.amazon.com/vpc/latest/peering/create-vpc-peering-connection.html#same-account-different-region). 

At the end of the peering process, you will need to update your route tables for the peering. The AWS docs on how to do this can be found [here](https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-routing.html).


### Security Groups and VPCs

When spinning up instances in different regions with boto3, issues can arise from the security group specified in the QuokkaClusterManager. An example would be the following error:

```
botocore.exceptions.ClientError: An error occurred (InvalidGroup.NotFound) when calling the RunInstances operation: The security group 'xyz’ does not exist in VPC 'abc’
```

Make sure to update the security group in order to allow VPC from other regions to access it. The process of how to do this is described in the AWS docs [here](https://docs.aws.amazon.com/vpc/latest/peering/vpc-peering-security-groups.html).


### Use of AWS Keys across Multiple regions

When using different key pairs for different regions, it is likely that the following error occurs:

```
botocore.exceptions.ClientError: An error occurred (InvalidKeyPair.NotFound) when calling the RunInstances operation: The key pair xxxxx does not exist
```

In order for this to work, we need to import the key that we want to use across the instances to all the regions that we want to connect using the multi-region cluster. An article on how to import AWS keys to other regions can be found [here](https://michael-ludvig.medium.com/re-using-ec2-ssh-key-pair-in-multiple-regions-982cb620a3ca). 

When importing the key to a different region, make sure to give it the same name as the key in the original region. The QuokkaClusterManager class does not distinguish between key names. This means that it will not be able to handle various key names.