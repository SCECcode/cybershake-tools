"""Integrated workflow generation."""

from __future__ import annotations

from typing import TYPE_CHECKING

from Pegasus.api import File, SubWorkflow, Workflow

from cybershake.constants import PP_STORAGE_DIR
from cybershake.exc import FullError
from cybershake.workflow.common import (
    add_config_metadata,
    add_local_replica,
    dependency,
    job,
    update_job,
    write_workflow,
)

if TYPE_CHECKING:
    from pathlib import Path

    from cybershake.workflow.pp import PP
    from cybershake.workflow.sgt import SGT
    from cybershake.workflow.stoch import Stoch


class Full:
    """Compose SGT, post-processing, and stochastic workflow generators."""

    def __init__(
        self,
        *,
        sgt: SGT,
        pp: PP,
        stoch: Stoch,
    ) -> None:
        """Store the stage generators used by the integrated workflow.

        Args:
            sgt (SGT): Configured SGT workflow generator.
            pp (PP): Configured post-processing workflow generator.
            stoch (Stoch): Configured stochastic workflow generator.
        """
        self.sgt = sgt
        self.pp = pp
        self.stoch = stoch
        self.config = sgt.config

    def create(
        self,
        *,
        workflow_file: Path,
        run_directory: Path,
        run_ids: tuple[int, ...],
        broadband_run_id: int | None,
    ) -> None:
        """Generate an integrated workflow.

        Args:
            workflow_file (Path): Destination workflow file.
            run_directory (Path): Directory containing workflow inputs and
                outputs.
            run_ids (tuple[int, ...]): Deterministic run IDs in the workflow.
            broadband_run_id (int | None): Linked broadband run ID, if enabled.

        Raises:
            FullError: If broadband generation lacks its linked run ID or
                stochastic frequency.
        """
        broadband_id = self._broadband_run_id(broadband_run_id)
        workflow = Workflow(workflow_file.stem, infer_dependencies=False)
        add_config_metadata(workflow, self.config)
        info, sgt_terminal, sgt_data_ready = self.sgt.add_to(
            workflow,
            run_ids=run_ids,
        )
        set_job_id = job("SetJobID", f"SetJobID_{info.site}_{info.run_id}")
        set_job_id.add_args(str(run_directory.resolve()), str(info.run_id))
        workflow.add_jobs(set_job_id)
        dependency(workflow, sgt_terminal, set_job_id)

        pp_info, pre_file, synthesis_file, products_file, post_file = (
            self.pp.create_sub_workflows(
                run_id=info.run_id,
                run_directory=run_directory,
            )
        )
        if pp_info.site != info.site:
            raise FullError(
                f"SGT run {info.run_id} belongs to {info.site}, "
                f"but post-processing belongs to {pp_info.site}"
            )

        pre = SubWorkflow(pre_file.name, _id=f"{info.site}_preWf")
        pre.add_args("--force", "-q", "--cleanup", "none")
        if self.pp.pp_site is not None:
            pre.add_args("-s", f"{self.pp.pp_site},shock,local")

        synthesis = SubWorkflow(
            synthesis_file.name,
            _id=f"{info.site}_synthesisWf_0",
        )
        synthesis.add_args(
            "--cluster",
            "horizontal" if self.pp.no_mpi_cluster else "label",
            "--force",
            "-q",
            "--cleanup",
            "none",
        )
        if self.pp.pp_site is not None:
            synthesis.add_args("-s", self.pp.pp_site)
        synthesis.add_args(
            "--output-sites",
            "shock",
            "--output-dir",
            str(PP_STORAGE_DIR / info.site / str(info.run_id)),
        )
        synthesis.add_dagman_profiles(category="subwf")

        post = SubWorkflow(post_file.name, _id=f"{info.site}_postWf")
        post.add_args("--force", "-q", "--cleanup", "none")
        workflow.add_jobs(pre, synthesis, post)
        for path in (pre_file, synthesis_file, post_file):
            add_local_replica(workflow, File(path.name), path)

        dependency(workflow, set_job_id, pre)
        dependency(workflow, sgt_terminal, pre)
        for ready in sgt_data_ready:
            if ready is not sgt_terminal:
                dependency(workflow, ready, pre)
        if self.pp.nonblocking_md5:
            dependency(workflow, set_job_id, synthesis)
            dependency(workflow, sgt_terminal, synthesis)
        else:
            dependency(workflow, pre, synthesis)

        stochastic = (
            self._add_stochastic(
                workflow,
                run_directory=run_directory,
                site=info.site,
                low_frequency_run_id=info.run_id,
                broadband_run_id=broadband_id,
                pre=pre,
                synthesis=synthesis,
            )
            if broadband_id is not None
            else None
        )

        if products_file is None:
            dependency(workflow, synthesis, post)
        else:
            products = SubWorkflow(
                products_file.name,
                _id=f"{info.site}_productsWf",
            )
            products.add_args("--force", "-q", "--cleanup", "none")
            workflow.add_jobs(products)
            add_local_replica(workflow, File(products_file.name), products_file)
            dependency(workflow, synthesis, products)
            dependency(workflow, products, post)
            if stochastic is not None:
                dependency(workflow, stochastic, post)

        write_workflow(workflow, workflow_file)

    def _broadband_run_id(
        self,
        broadband_run_id: int | None,
    ) -> int | None:
        """Validate and return the optional broadband run ID.

        Args:
            broadband_run_id (int | None): Linked broadband run ID.

        Returns:
            int | None: Linked run ID when broadband generation is enabled.

        Raises:
            FullError: If broadband generation lacks either required value.
        """
        if not self.config["broadband"]:
            return None
        if broadband_run_id is None or self.config["stoch_frequency"] is None:
            raise FullError(
                "Broadband workflow generation requires a linked run ID "
                "and stochastic frequency"
            )
        return broadband_run_id

    def _add_stochastic(
        self,
        workflow: Workflow,
        *,
        run_directory: Path,
        site: str,
        low_frequency_run_id: int,
        broadband_run_id: int,
        pre: SubWorkflow,
        synthesis: SubWorkflow,
    ) -> SubWorkflow:
        """Generate and attach the optional stochastic workflow.

        Args:
            workflow (Workflow): Integrated workflow receiving the nodes.
            run_directory (Path): Directory receiving generated workflows.
            site (str): CyberShake site short name.
            low_frequency_run_id (int): Deterministic run providing SGT data.
            broadband_run_id (int): Linked broadband run ID.
            pre (SubWorkflow): PP preparation workflow.
            synthesis (SubWorkflow): Deterministic synthesis workflow.

        Returns:
            SubWorkflow: Attached stochastic top-level workflow.

        """
        self.stoch.create(
            run_id=broadband_run_id,
            run_directory=run_directory,
            low_frequency_run_id=low_frequency_run_id,
        )
        stochastic_file = run_directory / f"CyberShake_Stoch_{site}_top.dax"
        stochastic = SubWorkflow(
            stochastic_file.name,
            _id=f"{site}_stochWf",
        )
        stochastic.add_args("--force", "-q", "--cleanup", "none")
        update_start = update_job(
            broadband_run_id,
            "SGT_INIT",
            "SGT_START",
            identifier=f"UpdateRun{broadband_run_id}_SGT_START",
        )
        update_pp = update_job(
            broadband_run_id,
            "SGT_START",
            "PP_INIT",
            identifier=f"UpdateRun{broadband_run_id}_PP_INIT",
        )
        workflow.add_jobs(update_start, update_pp, stochastic)
        add_local_replica(workflow, File(stochastic_file.name), stochastic_file)
        dependency(workflow, pre, update_start)
        dependency(workflow, update_start, update_pp)
        dependency(workflow, synthesis, stochastic)
        dependency(workflow, update_pp, stochastic)
        return stochastic
