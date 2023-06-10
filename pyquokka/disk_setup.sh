sudo mkfs.ext4 -F -E nodiscard $(sudo nvme list | grep 'Amazon EC2 NVMe Instance Storage' | awk '{ print $1 }');
sudo mount $(sudo nvme list | grep 'Amazon EC2 NVMe Instance Storage' | awk '{ print $1 }') $1
sudo chmod -R a+rw $1
