# cwm-worker-tests

Python module and CLI which provides integration tests and related tools for cwm-worker components

## Install

Create virtualenv

```
python3 -m venv venv
```

Install dependencies

```
venv/bin/python -m pip install -r requirements.txt
```

Install the cwm-worker-tests module

```
venv/bin/python -m pip install -e .
```

Install the cwm-worker-cluster module (it must be available in this directory)

```
venv/bin/python -m pip install -e ../cwm-worker-cluster
```

## Use

Activate virtualenv

```
. venv/bin/activate
```

Connect your kubectl to a cwm-worker cluster, following should show cwm-worker cluster nodes

```
kubectl get nodes
```

Additional requirements for some of the tests:

* cwm-worker-cluster repository is required in one of the following locations:
    * current working directory is cwm-worker-cluster/ - doesn't matter where cwm-worker-tests is
    * current working directory is cwm-worker-tests/ - cwm-worker-cluster should be at ../cwm-worker-cluster
* the distributed load test downloads some code from git, so you need to commit & push for changes to take effect

See the CLI help messages

```
cwm-worker-tests --help
```
