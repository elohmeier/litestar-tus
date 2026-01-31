from __future__ import annotations

from typing import TYPE_CHECKING

from litestar.di import Provide
from litestar.middleware import DefineMiddleware
from litestar.plugins import InitPluginProtocol

from litestar_tus.config import TUSConfig
from litestar_tus.controller import build_tus_controller
from litestar_tus.middleware import TUSMiddleware
from litestar_tus.protocols import StorageBackend

if TYPE_CHECKING:
    from litestar.config.app import AppConfig


class TUSPlugin(InitPluginProtocol):
    def __init__(self, config: TUSConfig | None = None) -> None:
        self._config = config or TUSConfig()

    def on_app_init(self, app_config: AppConfig) -> AppConfig:
        config = self._config

        if config.storage_backend is not None:
            storage = config.storage_backend
        else:
            from litestar_tus.backends.file import FileStorageBackend

            storage = FileStorageBackend(config.upload_dir)

        app_config.dependencies["tus_storage"] = Provide(
            lambda: storage, use_cache=True, sync_to_thread=False
        )

        controller_cls = build_tus_controller(config)
        app_config.route_handlers.append(controller_cls)

        app_config.middleware.append(
            DefineMiddleware(
                TUSMiddleware,
                path_prefix=config.path_prefix,
                max_size=config.max_size,
                extensions=config.extensions,
            )
        )

        app_config.signature_namespace.update(
            {
                "StorageBackend": StorageBackend,
            }
        )

        return app_config
