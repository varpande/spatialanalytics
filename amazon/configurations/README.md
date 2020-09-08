# Amazon EMR Software Configuration Settings
In Amazon EMR under "Create Cluster - Advanced Options" and under "Step 1: Software and Steps", copy and paste the values from the json files depending on the size of the cluster.

Make sure in "Step 2: Hardware" of Create Cluster the values in the json matches with your configuration

The default machines we used are m4.xlarge for master and r4.8xlarge as slave nodes. We only count the slave nodes in the cluster. Also the yarn-site configurations in the json files reflect the Task configuration values for r4.8xlarge. If you choose a different type of machine as slave node, change these values according to the values here: https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-hadoop-task-config.html

Note: We attached 100GB general purpose SSDs with each slave node, and these disks are mounted at /mnt by Amazon. The "spark.local.dir" parameter in the json files reflects this path. If you intent on running larger queries/datasets, you may want to attach a bigger SSD.
