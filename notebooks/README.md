# Demonstrator

## Set up

```shell
python3 -m venv --clear ./venv
pip install -r requirements.txt
python -m grpc_tools.protoc demonstrator.proto --python_out=./ --proto_path=./
jupyter notebook
```
