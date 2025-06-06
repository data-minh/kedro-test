import logging
from collections import Counter
from itertools import chain
from typing import Any, Dict, Set

from pluggy import PluginManager

from kedro.io import DataCatalog
from kedro.pipeline import Pipeline
from kedro.pipeline.node import Node
from kedro.runner import SequentialRunner
from kedro.runner.runner import run_node



logger = logging.getLogger(__name__)


# MỚI: Hàm kiểm tra điều kiện skip, nhận vào node và catalog
def _should_skip_node(node: Node, catalog: DataCatalog) -> bool:
    """
    Hàm này chứa logic để quyết định có skip một node hay không.
    Nó sẽ được gọi cho mỗi node trong pipeline.

    Args:
        node: Đối tượng node đang được xem xét.
        catalog: DataCatalog chứa thông tin về tất cả các dataset.

    Returns:
        True nếu node cần được skip, False nếu không.
    """
    # --- ĐẶT LOGIC KIỂM TRA CỦA BẠN VÀO ĐÂY ---

    # VÍ DỤ: Skip node nếu bất kỳ output dataset nào của nó
    # có config `skip_on_rerun: true` trong catalog.
    
    for output_name in node.outputs:
        try:
            # Lấy đối tượng dataset từ catalog
            dataset_obj = catalog._datasets[output_name]
            
            # Truy cập vào config 'metadata' (bạn có thể dùng tên khác)
            # Giả định rằng bạn định nghĩa nó trong catalog.yml
            metadata_config = getattr(dataset_obj, "metadata", {})
            
            if metadata_config.get("skip_on_rerun", False):
                # Nếu config là True, kiểm tra xem dataset đã tồn tại chưa
                if catalog.exists(output_name):
                    logger.info(
                        f"Node '{node.name}' sẽ được skip vì output '{output_name}' "
                        f"có 'skip_on_rerun: true' và đã tồn tại."
                    )
                    return True # Quyết định skip
        except (KeyError, AttributeError):
            # Bỏ qua nếu dataset không có trong catalog hoặc không có thuộc tính metadata
            continue

    # Logic kiểm tra trong ngày của bạn có thể đặt ở đây
    # Ví dụ: if node.name == "my_daily_node" and has_run_today("my_daily_node"):
    #           return True
            
    return False # Mặc định là không skip


# THAY ĐỔI: Đổi tên Runner để phản ánh đúng chức năng mới
class ConfigurableSkippingRunner(SequentialRunner):
    """
    Runner này chạy pipeline tuần tự và cho phép skip các node
    dựa trên điều kiện được cấu hình, trước khi thực thi chúng.
    """

    def _get_nodes_to_skip(self, pipeline: Pipeline, catalog: DataCatalog) -> Set[Node]:
        """Xác định tập hợp các node cần skip dựa trên điều kiện."""
        nodes_to_skip = set()
        
        for node in pipeline.nodes:
            # Nếu node đã nằm trong danh sách skip (do node cha của nó bị skip)
            # thì không cần kiểm tra lại
            if node in nodes_to_skip:
                continue

            # Kiểm tra điều kiện skip cho node hiện tại
            if _should_skip_node(node, catalog):
                # Nếu node này cần skip, tất cả các node con của nó cũng phải skip
                downstream_nodes = set(pipeline.from_nodes(node.name).nodes)
                nodes_to_skip.update(downstream_nodes)
        
        return nodes_to_skip


    # THAY ĐỔI: Viết lại phương thức _run để tích hợp logic skip mới
    def _run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager,
        session_id: str = None,
    ) -> None:
        """Phương thức thực thi pipeline với logic skip được tùy chỉnh."""
        
        # 1. Xác định tất cả các node cần skip ngay từ đầu
        nodes_to_skip = self._get_nodes_to_skip(pipeline, catalog)
        
        # 2. Chạy các node còn lại
        nodes_to_run = [node for node in pipeline.nodes if node not in nodes_to_skip]
        
        if nodes_to_skip:
            logger.warning(
                f"Các node sau sẽ được bỏ qua (skipped): "
                f"{[node.name for node in nodes_to_skip]}"
            )

        # Sử dụng lại logic chạy tuần tự của SequentialRunner gốc
        # cho các node không bị skip
        for i, node in enumerate(nodes_to_run):
            try:
                # `run_node` là hàm nội bộ của Kedro, nó sẽ xử lý các hook
                # before_node_run, before_dataset_loaded, etc.
                run_node(node, catalog, hook_manager, self._is_async, session_id)
            except Exception:
                logger.error(f"Node '{node.name}' đã thất bại.")
                # Nếu bạn muốn dừng khi có lỗi, hãy raise exception ở đây
                # raise
                continue # Hoặc bỏ qua và chạy tiếp
                
            logger.info(f"Hoàn thành node {i + 1}/{len(nodes_to_run)}: {node.name}")
        
        logger.info("Thực thi pipeline hoàn tất.")

    # Các phương thức run, _summary, _suggest_resume_scenario... có thể giữ nguyên
    # hoặc lược bỏ nếu không cần đến logic của SoftFailRunner nữa.
    # Để đơn giản, tôi sẽ giữ lại phương thức `run` chính.

    def run(
        self,
        pipeline: Pipeline,
        catalog: DataCatalog,
        hook_manager: PluginManager = None,
        session_id: str = None,
    ) -> Dict[str, Any]:
        """
        Điểm khởi đầu để chạy pipeline.
        """
        # Các bước kiểm tra input của pipeline được giữ nguyên
        unsatisfied = pipeline.inputs() - set(catalog.list())
        if unsatisfied:
            raise ValueError(
                f"Pipeline input(s) {unsatisfied} not found in the DataCatalog"
            )

        self._run(pipeline, catalog, hook_manager, session_id)

        # Trả về output rỗng vì chúng ta không xử lý output chưa đăng ký trong ví dụ này
        return {}