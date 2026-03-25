(function () {
  const MIN_COL_WIDTH = 60;

  function initTable(table) {
    if (!table || table.dataset.avResizerInit === "1") return;
    const colgroup = table.querySelector("colgroup");
    if (!colgroup) return;
    const cols = Array.from(colgroup.querySelectorAll("col"));
    if (!cols.length) return;

    const resizers = Array.from(table.querySelectorAll(".av-col-resizer"));
    if (!resizers.length) return;

    resizers.forEach((handle) => {
      handle.addEventListener("mousedown", (e) => {
        e.preventDefault();
        e.stopPropagation();

        const idxRaw = handle.getAttribute("data-col-index") || "";
        const idx = parseInt(idxRaw, 10);
        if (!Number.isFinite(idx) || idx < 0 || idx >= cols.length) return;

        const col = cols[idx];
        const startX = e.clientX;
        const th = handle.closest("th");
        const startWidth =
          (th && th.getBoundingClientRect().width) ||
          parseInt(col.style.width || "0", 10) ||
          MIN_COL_WIDTH;

        document.body.style.cursor = "col-resize";

        function onMove(ev) {
          const dx = ev.clientX - startX;
          const next = Math.max(MIN_COL_WIDTH, Math.round(startWidth + dx));
          col.style.width = `${next}px`;
        }

        function onUp() {
          document.body.style.cursor = "";
          window.removeEventListener("mousemove", onMove);
          window.removeEventListener("mouseup", onUp);
        }

        window.addEventListener("mousemove", onMove);
        window.addEventListener("mouseup", onUp);
      });
    });

    table.dataset.avResizerInit = "1";
  }

  function findAndInit() {
    const table = document.getElementById("av-homework-table");
    if (table) initTable(table);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", findAndInit);
  } else {
    findAndInit();
  }

  const observer = new MutationObserver(() => findAndInit());
  observer.observe(document.documentElement, { childList: true, subtree: true });
})();
