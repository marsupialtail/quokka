import os
import time
import boto3
import subprocess
import multiprocessing
import pyquokka
import ray
import json

class QuokkaCluster:
    def __init__(self, public_ips, private_ips, instance_ids, cpu_count_per_instance) -> None:
        
        self.num_node = len(public_ips)
        self.public_ips = {}
        self.private_ips = {}
        self.instance_ids = {}

        for node in range(self.num_node):
            self.public_ips[node] = public_ips[node]
            self.private_ips[node] = private_ips[node]
            self.instance_ids[node] = instance_ids[node]
        
        self.state = "running"
        self.cpu_count = cpu_count_per_instance
        self.leader_public_ip = self.public_ips[0]
        self.leader_private_ip = self.private_ips[0]

        pyquokka_loc = pyquokka.__file__.replace("__init__.py","")
        # connect to that ray cluster
        ray.init(address='ray://' + str(self.leader_public_ip) + ':10001', runtime_env={"py_modules":[pyquokka_loc]})

    @classmethod
    def from_json(cls, json_file):
        def str_key_to_int(d):
            return {int(i):d[i] for i in d}
        stuff = json.load(open(json_file,"r"))
        return cls(str_key_to_int(stuff["public_ips"]), str_key_to_int(stuff["private_ips"]), str_key_to_int(stuff["instance_ids"]), int(stuff["cpu_count_per_instance"]))
    
    def to_json(self, output = "cluster.json"):
        json.dump({"public_ips":self.public_ips, "private_ips":self.private_ips,"instance_ids":self.instance_ids,"cpu_count_per_instance":self.cpu_count},open(output,"w"))
    



class LocalCluster:
    def __init__(self) -> None:
        self.cpu_count = multiprocessing.cpu_count()
        self.leader_public_ip = "localhost"
        self.leader_private_ip = "localhost"

        # we assume you have pyquokka installed, and we are going to spin up a ray cluster locally
        ray.init(ignore_reinit_error=True)

class QuokkaClusterManager:

    def __init__(self, key_name = "oregon-neurodb", key_location = "/home/ziheng/Downloads/oregon-neurodb.pem", security_group= "sg-0770c1101ab26fba2") -> None:
        self.key_name = key_name
        self.key_location = key_location
        self.security_group = security_group
        pass

    def launch_all(self, command, ips, error = "Error"):
        commands = ["ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i " + self.key_location + " ubuntu@" + str(ip) + " " + command for ip in ips]
        processes = [subprocess.Popen(command, close_fds=True, shell=True) for command in commands]
        return_codes = [process.wait() for process in processes]
        if sum(return_codes) != 0:
            raise Exception(error)

    def check_instance_alive(self, public_ip):
        z = os.system("ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i " + self.key_location + " ubuntu@" + public_ip)
        if z == 0:
            return False
        else:
            return True

    def launch_new_instances(self, aws_access_key, aws_access_id, num_instances = 1, instance_type = "i3.2xlarge", requirements = ["ray==1.12.0"]):
        ec2 = boto3.client("ec2")
        vcpu_per_node = ec2.describe_instance_types(InstanceTypes=['i3.2xlarge'])['InstanceTypes'][0]['VCpuInfo']['DefaultVCpus']
        waiter = ec2.get_waiter('instance_running')

        # important 2 things:
        # this instance needs to have all the things installed on it
        # this instance needs to have the right tcp permissions
        res = ec2.run_instances(ImageId="ami-0ac46f512c1730e1a", InstanceType = instance_type, SecurityGroupIds = [self.security_group], KeyName=self.key_name ,MaxCount=num_instances, MinCount=num_instances)
        instance_ids = [res['Instances'][i]['InstanceId'] for i in range(num_instances)] 
        waiter.wait(InstanceIds=instance_ids)
        a = ec2.describe_instances(InstanceIds = instance_ids)
        public_ips = [a['Reservations'][0]['Instances'][i]['PublicIpAddress'] for i in range(num_instances)]
        private_ips = [a['Reservations'][0]['Instances'][i]['PrivateIpAddress'] for i in range(num_instances)]

        count = 0
        while True:
            z = [os.system("ssh -oStrictHostKeyChecking=no -oConnectTimeout=2 -i " + self.key_location + " ubuntu@" + public_ip +" time") for public_ip in public_ips]
            if sum(z) == 0:
                break
            else:
                count += 1
                if count == 6:
                    raise Exception("Couldn't connect to new instance in 30 seconds.")
                time.sleep(5)
        
        print(public_ips)
        self.launch_all("aws configure set aws_secret_access_key " + str(aws_access_key), public_ips, "Failed to set AWS access key")
        self.launch_all("aws configure set aws_access_key_id " + str(aws_access_id), public_ips, "Failed to set AWS access id")
        self.launch_all("redis-6.2.6/src/redis-server redis-6.2.6/redis.conf --port 6800 --protected-mode no&", public_ips, "Failed to start Redis server on new worker")

        for req in requirements:
            assert type(req) == str
            try:
                self.launch_all("pip3 install " + req, public_ips, "Failed to install " + req)
            except:
                pass

        return public_ips, private_ips, instance_ids, vcpu_per_node

    def create_cluster(self, aws_access_key, aws_access_id, num_instances, instance_type = "i3.2xlarge", requirements=[]):
        
        start_time = time.time()
        public_ips, private_ips, instace_ids, vcpu_per_node = self.launch_new_instances(aws_access_key, aws_access_id, num_instances, instance_type, requirements)
        print("Launching of EC2 on-demand instances used: ", time.time() - start_time)

        leader_public_ip = public_ips[0]
        leader_private_ip = private_ips[0]
        z = os.system("ssh -oStrictHostKeyChecking=no -i " + self.key_location + " ubuntu@" + leader_public_ip + 
        " /home/ubuntu/.local/bin/ray start --head --port=6379")
        if z != 0:
            raise Exception("failed to start ray head node")
        
        command ="/home/ubuntu/.local/bin/ray start --address='" + str(leader_private_ip) + ":6379' --redis-password='5241590000000000'"
        self.launch_all(command, public_ips, "ray workers failed to connect to ray head node")
        
        print("Trying to set up spill dir.")
        self.launch_all("sudo mkdir /data", public_ips, "failed to make temp spill directory")
        
        if "i3" in instance_type: # use a more sophisticated policy later
            self.launch_all("sudo mkfs.ext4 -E nodiscard /dev/nvme0n1;", public_ips, "failed to format nvme ssd")
            self.launch_all("sudo mount /dev/nvme0n1 /data;", public_ips, "failed to mount nvme ssd")
        
        self.launch_all("sudo chmod -R a+rw /data/", public_ips, "failed to give spill dir permissions")

        print("Quokka cluster started, coordinator IP address: ", leader_public_ip)
        return QuokkaCluster(public_ips, private_ips, instace_ids, vcpu_per_node)

    def stop_cluster(self, quokka_cluster):
        ec2 = boto3.client("ec2")
        instance_ids = list(quokka_cluster.instance_ids.values())
        ec2.stop_instances(InstanceIds = instance_ids)
        while True:
            time.sleep(0.1)
            a = ec2.describe_instances(InstanceIds = instance_ids)
            states = [a['Reservations'][0]['Instances'][i]['State']['Name'] for i in range(len(instance_ids))]
            if "running" in states:
                continue
            else:
                break
        quokka_cluster.state = "stopped"
        
        
    def terminate_cluster(self, quokka_cluster):
        ec2 = boto3.client("ec2")
        instance_ids = list(quokka_cluster.instance_ids.values())
        ec2.terminate_instances(InstanceIds = instance_ids)
        while True:
            time.sleep(0.1)
            a = ec2.describe_instances(InstanceIds = instance_ids)
            states = [a['Reservations'][0]['Instances'][i]['State']['Name'] for i in range(len(instance_ids))]
            if "running" in states:
                continue
            else:
                break
        del quokka_cluster