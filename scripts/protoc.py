import importlib.resources
import pathlib
import warnings

import google.type.date_pb2
import grpc_tools

_ROOT_DIR = pathlib.Path(__file__).parent.parent.absolute()
_GOOGLE_COMMON_PROTOS_ROOT_DIR = (
    pathlib.Path(google.type.date_pb2.__file__).parents[2].absolute()
)
_GRPC_PROTOS_INCLUDE = importlib.resources.files(grpc_tools) / "_proto"
_SRC_DIR = _ROOT_DIR / "protos"
_OUT_DIR = _ROOT_DIR / "ptars_protos"


def run_protoc(arguments: list[str]):
    try:
        import grpc_tools.protoc

        return_code = grpc_tools.protoc.main(arguments)
        if return_code != 0:
            raise RuntimeError("Could not generate proto")

    except ImportError:
        warnings.warn("Using system version of protoc", stacklevel=2)
        import subprocess  # nosec B404

        subprocess.run(arguments, check=True)  # nosec B603


def main():
    _OUT_DIR.mkdir(parents=True, exist_ok=True)

    proto_files = [x.as_posix() for x in _SRC_DIR.glob("**/*.proto")]
    proto_args = [
        "protoc",
        f"--proto_path={_GOOGLE_COMMON_PROTOS_ROOT_DIR}",
        f"--proto_path={_GRPC_PROTOS_INCLUDE}",
        f"--proto_path={_SRC_DIR}",
        f"--python_out={_ROOT_DIR}",
        f"--pyi_out={_ROOT_DIR}",
    ] + proto_files
    print(" ".join(proto_args))
    run_protoc(proto_args)
    (_OUT_DIR / "__init__.py").touch()


if __name__ == "__main__":
    main()
