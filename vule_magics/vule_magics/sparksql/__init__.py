import IPython.display as ipd
from .sparksql import SparkSQL


def load_ipython_extension(ipython):
    ipd.display_javascript(
        "try {"
            "require(['notebook/js/codecell'], function (codecell) {"
                "codecell.CodeCell.options_default.highlight_modes['magic_text/x-mssql'] = { 'reg': [/%%sql/] };"
                "Jupyter.notebook.events.one('kernel_ready.Kernel', function () {"
                    "Jupyter.notebook.get_cells().map(function (cell) {"
                        "if (cell.cell_type == 'code') { cell.auto_highlight(); }"
                    "});"
                "});"
            "});"
        "} catch(e) {}"
    , raw=True)
    ipd.display(ipd.Javascript("""
        var vuleWidgetIconUpdateEventId = localStorage.getItem('vuleWidgetIconUpdateEventId');
        if (vuleWidgetIconUpdateEventId !== null) {
            clearInterval(vuleWidgetIconUpdateEventId)
        }
        vuleWidgetIconUpdateEventId = setInterval(function () {
            document.querySelectorAll(".fa-scatter-chart").forEach(element => {
                console.log(element);
                let scatterIcon = `<span><svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" version="1.1" width="13px" height="13px" viewBox="-0.5 -0.5 153 122" style="vertical-align: -1.5px"><defs/><g><rect x="0" y="0" width="20" height="120" rx="5" ry="5" fill="currentColor" pointer-events="all"/><ellipse cx="52" cy="65" rx="15" ry="15" fill="currentColor" pointer-events="all"/><ellipse cx="103" cy="59" rx="15" ry="15" fill="currentColor" pointer-events="all"/><ellipse cx="77" cy="20" rx="15" ry="15" fill="currentColor" pointer-events="all"/><ellipse cx="127.5" cy="17.5" rx="17.5" ry="17.5" fill="currentColor" pointer-events="all"/><rect x="67" y="35" width="20" height="150" rx="5" ry="5" fill="currentColor" transform="rotate(90,77,110)" pointer-events="all"/></g></svg></span>`;
                element.parentNode.innerHTML = scatterIcon;
            });
        }, 50)

        localStorage.setItem('vuleWidgetIconUpdateEventId', vuleWidgetIconUpdateEventId)
    """))
    ipython.register_magics(SparkSQL)