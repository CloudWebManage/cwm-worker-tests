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

Make sure cwm-worker-cluster is a sibling directory of cwm-worker-tests

See the CLI help messages

```
cwm-worker-tests --help
```
