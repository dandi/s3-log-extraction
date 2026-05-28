import importlib.util
import pathlib
import sys
import types


_REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
_PACKAGE_ROOT = _REPOSITORY_ROOT / 'src' / 's3_log_extraction'
_CONFIG_ROOT = _PACKAGE_ROOT / 'config'
_PACKAGE_NAME = '_isolated_s3_log_extraction'
_CONFIG_PACKAGE_NAME = f'{_PACKAGE_NAME}.config'


def _load_config_package() -> types.ModuleType:
    package = types.ModuleType(_PACKAGE_NAME)
    package.__path__ = [str(_PACKAGE_ROOT)]
    sys.modules[_PACKAGE_NAME] = package

    spec = importlib.util.spec_from_file_location(
        _CONFIG_PACKAGE_NAME,
        _CONFIG_ROOT / '__init__.py',
        submodule_search_locations=[str(_CONFIG_ROOT)],
    )
    assert spec is not None
    assert spec.loader is not None

    module = importlib.util.module_from_spec(spec)
    sys.modules[_CONFIG_PACKAGE_NAME] = module
    spec.loader.exec_module(module)
    return module


def test_get_cache_subdirectory_creates_named_directory(tmp_path: pathlib.Path) -> None:
    config = _load_config_package()
    cache_directory = tmp_path / 'cache'
    cache_directory.mkdir()

    records_directory = config.get_cache_subdirectory(cache_directory=cache_directory, name='records')

    assert records_directory == cache_directory / 'records'
    assert records_directory.is_dir()


def test_config_module_no_longer_exports_specialized_directory_helpers() -> None:
    config = _load_config_package()

    assert 'get_records_directory' not in config.__all__
    assert 'get_ip_cache_directory' not in config.__all__
    assert 'get_summary_directory' not in config.__all__
    assert not hasattr(config, 'get_records_directory')
    assert not hasattr(config, 'get_ip_cache_directory')
    assert not hasattr(config, 'get_summary_directory')
