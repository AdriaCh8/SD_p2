# Install requirements
pip3 install -r requirements.txt

python3 -m grpc_tools.protoc --proto_path=. --grpc_python_out=. --pyi_out=. --python_out=. ./KVStore/protos/*.proto