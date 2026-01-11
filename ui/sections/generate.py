# run button + execution
import streamlit as st
import yaml
from pathlib import Path
import tempfile
import subprocess
import sys
import re
import time

ANSI_ESCAPE_RE = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


def apply_global_dates(cfg):
    start = cfg["defaults"]["dates"]["start"]
    end = cfg["defaults"]["dates"]["end"]

    for section in ["sales", "stores", "promotions", "dates", "exchange_rates"]:
        if section not in cfg:
            continue
        cfg[section].setdefault("dates", {})
        cfg[section]["dates"]["start"] = start
        cfg[section]["dates"]["end"] = end


def render_generate(cfg, errors):
    st.subheader("6️⃣ Generate")

    sales = cfg["sales"]
    force_regen = st.session_state.get(
        "force_regenerate_dimensions", set()
    )

    # --------------------------------------------------
    # Summary
    # --------------------------------------------------
    summary = f"""
**This will generate:**
- **{sales['total_rows']:,}** sales rows
- **{cfg['customers']['total_customers']:,}** customers
- **{cfg['products']['num_products']:,}** products
- Output format: **{sales['file_format'].upper()}**
"""
    st.markdown(summary)

    if force_regen:
        if "all" in force_regen:
            st.info("🔄 **Forced regeneration:** All dimensions")
        else:
            labels = [
                dim.replace("_", " ").title()
                for dim in sorted(force_regen)
            ]
            st.info(
                f"🔄 **Forced regeneration:** {', '.join(labels)}"
            )

    # --------------------------------------------------
    # Run pipeline
    # --------------------------------------------------
    if st.button("▶ Generate Data", type="primary"):
        if errors:
            st.error("Fix validation errors before running.")
            return

        apply_global_dates(cfg)

        force_regen = st.session_state.get(
            "force_regenerate_dimensions", set()
        )

        st.info("Running pipeline...")
        log_area = st.empty()

        # ----------------------------------------------
        # Write temp config
        # ----------------------------------------------
        tmp_dir = tempfile.TemporaryDirectory()
        cfg_path = Path(tmp_dir.name) / "config.yaml"

        with open(cfg_path, "w", encoding="utf-8") as f:
            yaml.safe_dump(cfg, f, sort_keys=False)

        # ----------------------------------------------
        # Resolve project root + main.py explicitly
        # ----------------------------------------------
        project_root = Path(__file__).resolve().parents[2]
        main_py = project_root / "main.py"

        cmd = [
            sys.executable,
            "-u",  # unbuffered stdout
            str(main_py),
            "--config",
            str(cfg_path),
        ]

        if force_regen:
            cmd.extend(["--regen-dimensions", *sorted(force_regen)])

        # ----------------------------------------------
        # Launch subprocess
        # ----------------------------------------------
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        # ----------------------------------------------
        # Stream logs live
        # ----------------------------------------------
        output = ""

        while True:
            line = process.stdout.readline()
            if not line and process.poll() is not None:
                break

            if line:
                clean = ANSI_ESCAPE_RE.sub("", line)
                output += clean
                log_area.code(output)

        return_code = process.wait()

        if return_code == 0:
            st.success("Pipeline completed successfully.")
        else:
            st.error(f"Pipeline failed with exit code {return_code}.")
