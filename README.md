# Terraform Provider for Apache Pinot

Manage Pinot **schemas** and **tables** (OFFLINE/REALTIME) via the Pinot Controller API — from plain JSON you keep in Git.

> Built on the [Terraform Plugin Framework](https://github.com/hashicorp/terraform-plugin-framework).

---

## Requirements

- [Go](https://go.dev/dl/) **>= 1.22** (1.23+ also OK)
- [Terraform](https://developer.hashicorp.com/terraform/downloads) **>= 1.4**
- A reachable **Pinot Controller** (e.g. `http://localhost:9000`)

---

## Install / Build

```bash
# clone
git clone https://github.com/<you-or-org>/terraform-provider-pinot.git
cd terraform-provider-pinot

# build the provider binary
go build -o ./bin/terraform-provider-pinot
