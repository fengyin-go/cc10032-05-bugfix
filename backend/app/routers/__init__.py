"""业务路由汇总：通用资源路由 + 迟到补数专用路由。

专用路由必须比通用路由先注册，否则 ``/api/late_handler/imports`` 会先命中
``/api/{module}/{entry_id}`` 并被当作整数 id 解析失败。
"""
from __future__ import annotations

from app.routers import late, resource

ROUTERS = [late, resource]
