from .container_operator_builder import ContainerOperatorBuilder
from airflow.providers.singularity.operators.singularity import SingularityOperator


class SingularityOperatorBuilder(ContainerOperatorBuilder):
    """
    Currently untested!!
    """

    def build_mounts(self, mounts: list[str]):
        """Singularity operator uses raw mount strings as they are, /path/in/host:/path/in/container"""
        return mounts

    def _define_base_operator(self) -> SingularityOperator:
        return SingularityOperator(
            image=self.image,
            command=self.command,
            volumes=self.mounts,
            task_id=self.operator_id,
            task_display_name=self.display_name,
            auto_remove=True,
        )
