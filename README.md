# Jupyter Lab extension setup
1. Install cookiecutter
    ```bash
    pip install cookiecutter
    ```
1. Download template
    ```bash
    cookiecutter https://github.com/jupyterlab/extension-cookiecutter-ts
    ```
1. Build extension
    ```bash
    cd <extension-dir>
    pip install -e .
    jupyter labextension develop . --overwrite
    jlpm run build
    ```
1. Then run line to watch
    ```bash
    jupyter lab --watch
    ```

1. Require
`pandas, pyspark, plotly, ipywidget`