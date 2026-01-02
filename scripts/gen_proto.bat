@echo off
REM Generate Python gRPC stubs from stream_list.proto

echo Generating gRPC Python stubs from stream_list.proto...

python -m grpc_tools.protoc -I . --python_out=. --grpc_python_out=. stream_list.proto

if %ERRORLEVEL% EQU 0 (
    echo Successfully generated stream_list_pb2.py and stream_list_pb2_grpc.py
) else (
    echo Error generating proto files
    exit /b 1
)


