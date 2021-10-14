# cwm-worker-tests

Python module and CLI which provides integration tests and related tools for cwm-worker components

## Install

Follow the installation in cwm-worker-cluster repository

Activate and use the cwm-worker-cluster Python virtualenv which also install cwm-worker-tests

## Use

Activate virtualenv

```
. ../cwm-worker-cluster/venv/bin/activate
```

Connect to a cluster:

```
eval "$(cwm-worker-cluster cluster connect CLUSTER_NAME)"
```

See the CLI help messages (also available in cwm-worker-cluster/CLI.md)

```
cwm-worker-tests --help
```

The distributed load test downloads some code from git, so you need to commit & push for changes to take effect
