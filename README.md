# qBachTorRick

*insert project description here*

## Quickstart

`main.py` will init a single BitTorrent node. A config file is required. Sample `config.json`:

```json
{
  "advertise_ip": "public.reachable.ip.address",
  "port": 6881,
  "peer_port": 9000,
  "bootstrap": ["public.reachable.ip.address", 6881]
}
```

`bootstrap` is not required for the very first node.

To run:

```sh
python3 main.py config.json
```

## Deployment

A Terraform/OpenTofu plan is provided in `infra/` to quickly spin up a DHT network.

First, define variables in `infra/terraform.tfvars`:

```
aws_region          = "us-west-1"
instance_type       = "t3.micro"
ssh_key_name        = "qbachtorrick"
ssh_public_key_path = "~/.ssh/qbachtorrick.pub"
ssh_cidr            = "203.0.113.1/32"
dht_port            = 6881
```

Replace `ssh_public_key_path` with the local path to your local SSH key (or create a new keypair). `ssh_cidr` should be your public IP address to allow SSH from your machine to these instances.

Once completed, apply the plan:

```sh
cd infra/
tofu init
tofu validate
tofu plan
tofu apply
```

To view public IPs to connect to the network, use `tofu output public_ips`.

Once completed, these instances will not automatically run the program. You'll need to do this manually.

To remove these resources:

```sh
tofu destroy
```
