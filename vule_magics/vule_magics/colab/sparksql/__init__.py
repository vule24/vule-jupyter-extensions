from IPython.core.magic import Magics, magics_class, cell_magic, line_magic
from IPython.core.magic_arguments import argument, magic_arguments, parse_argstring
from IPython import display as ipd
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from google.colab import output as colab_output
from string import Formatter
import ipywidgets as w
import pandas as pd
import numpy as np
import copy
import time

pd.options.plotting.backend = "plotly"

def load_ipython_extension(ipython):
    colab_output.enable_custom_widget_manager()
    ipython.register_magics(SparkSQL)

@magics_class
class SparkSQL(Magics):

    @property
    def spark(self):
        return SparkSession._instantiatedSession

    @magic_arguments()
    @argument('dataframe', metavar='DF', type=str, nargs='?')
    @argument('-n', '--num-rows', type=int, default=100)
    @argument('-c', '--classic', action='store_true', default=False)
    @cell_magic
    def sql(self, line, cell):
        self.create_temp_view_for_available_dataframe()
        
        args = parse_argstring(self.sql, line)
        query_str = rf'{self.format_fillin_pyvar(cell)}'
        
        sdf = self.spark.sql(query_str)
        if args.dataframe:
            self.shell.user_ns.update({args.dataframe: sdf})
        if not args.classic:
            return generate_output_widget(sdf, num_rows=args.num_rows, export_table_name=args.dataframe or None)
        else:
            return generate_classic_table(sdf, num_rows=args.num_rows)


    @magic_arguments()
    @argument('dataframe', metavar='DF', type=str, nargs='?')
    @argument('-n', '--num-rows', type=int, default=20)
    @argument('-c', '--classic', action='store_true', default=False)
    @line_magic
    def show(self, line):
        args = parse_argstring(self.sql, line)
        if not args.dataframe:
            raise ValueError('dataframe is required. Eg. `%show <dataframe>`')
        sdf = self.shell.user_ns.get(args.dataframe, None)
        if not sdf:
            raise NameError(f"Name '{args.dataframe}' is not defined")
        if not args.classic:
            return generate_output_widget(sdf, num_rows=args.num_rows, export_table_name=args.dataframe)
        else:
            return generate_classic_table(sdf, num_rows=args.num_rows)


    def create_temp_view_for_available_dataframe(self):
        for k, v in self.shell.user_ns.items():
            v.createOrReplaceTempView(k) if isinstance(v, DataFrame) else None

    def format_fillin_pyvar(self, source):
        params = [fn for _, fn, _, _ in Formatter().parse(source) if fn]
        params_values = {}
        for param in params:
            value = self.shell.user_ns.get(param, None)
            if not value:
                raise NameError("name '{}' is not defined".format(param))
            params_values.update({param: value})
        return source.format(**params_values) 


def generate_classic_table(sdf, num_rows):
     with pd.option_context(
        'display.max_rows', None, 
        'display.max_columns', None, 
        'display.max_colwidth', None
    ):
        return ipd.HTML(
            "<div style='max-height: 650px'>" +
            sdf.limit(num_rows).toPandas().style
                .format(na_rep='null', precision=3, thousands=",", decimal=".")
                .set_table_styles([
                    {'selector': 'thead th', 'props': 'position: sticky; top: 0; z-index: 1; background-color: var(--jp-layout-color0); border-bottom: var(--jp-border-width) solid var(--jp-border-color1) !important;'},
                    {'selector': 'thead th:first-child', 'props': 'position: sticky; left: 0; z-index: 2; background-color: var(--jp-layout-color0);'},
                    {'selector': 'tbody th', 'props': 'position: sticky; left: 0; z-index: 1; background-color: inherit;'},
                ])
                .set_table_attributes(
                    'style="border-collapse:separate"'
                )
                .to_html() +
            "</div>"
        )


def generate_table(sdf, num_rows):
    with pd.option_context(
        'display.max_rows', None, 
        'display.max_columns', None, 
        'display.max_colwidth', None
    ):
        dataframe = copy.deepcopy(sdf.limit(num_rows).toPandas())
        return (
            dataframe,
            ipd.HTML(
                "<div style='max-height: 650px'>" +
                dataframe.style
                    .format(na_rep='null', precision=3, thousands=",", decimal=".")
                    .set_table_styles([
                        {'selector': 'thead th', 'props': 'position: sticky; top: 0; z-index: 1; background-color: var(--jp-layout-color0); border-bottom: var(--jp-border-width) solid var(--jp-border-color1) !important;'},
                        {'selector': 'thead th:first-child', 'props': 'position: sticky; left: 0; z-index: 2; background-color: var(--jp-layout-color0);'},
                        {'selector': 'tbody th', 'props': 'position: sticky; left: 0; z-index: 1; background-color: inherit;'},
                    ])
                    .set_table_attributes(
                        'style="border-collapse:separate"'
                    )
                    .to_html() +
                "</div>"
            )
        )

def plot(output_widget, current_render, template, dataframe, x, y, agg, logx, logy):
    plot_df = None
    plot_y = None
    if agg == '-':
        plot_df = dataframe
        plot_y = y
    else:
        if x != y:
            plot_df = dataframe.groupby(x).agg({y: agg}).reset_index().rename(columns={y: f'{agg}_{y}'})
            plot_y = f'{agg}_{y}'
        else:
            plot_df = dataframe
            plot_df[f'{agg}_{y}'] = plot_df[y]
            plot_df = plot_df.groupby(x).agg({f'{agg}_{y}': agg}).reset_index()
            plot_y = f'{agg}_{y}'
        
 
    with output_widget:     
        ipd.clear_output()
        fig = plot_df.plot(
            kind=current_render, 
            x=x,
            y=plot_y, 
            template=template,
        )
        if logx:
            fig.update_layout(xaxis_type="log")
        if logy:
            fig.update_layout(yaxis_type="log")
        time.sleep(0.5) # wait for plotly completely ready before render
        ipd.display(fig)


def generate_output_widget(sdf, num_rows, export_table_name=None):
    dataframe, table_html = generate_table(sdf, num_rows=num_rows)
    state = dict(
        current_render='table',
        template='plotly_dark' if colab_output.eval_js('document.documentElement.matches("[theme=dark]")') else None
    )
    
    # elements
    layout_btn_render_type = w.Layout(
        width='auto',
        margin='1px 2px 2px 2px')
 
    btn_table = w.Button(
        disabled=False,
        button_style='',
        icon='table',
        layout=layout_btn_render_type)
    btn_chart_line = w.Button(
        disabled=False,
        button_style='',
        icon='line-chart',
        layout=layout_btn_render_type)
    btn_chart_bar = w.Button(
        disabled=False,
        button_style='',
        icon='bar-chart',
        layout=layout_btn_render_type)
    btn_chart_scatter = w.Button(
        disabled=False,
        button_style='',
        icon='scatter-chart',
        layout=layout_btn_render_type)
    btn_save_csv = w.Button(
        description='Save as CSV',
        disabled=False,
        button_style='warning',
        icon='save',
        layout=w.Layout(width='auto', margin='1px 2px 2px 2px'))
    dropdown_x = w.Dropdown(
        options=dataframe.columns,
        description='X:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    dropdown_y = w.Dropdown(
        options=dataframe.select_dtypes(include=np.number).columns.to_list(),
        description='Y:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    dropdown_aggregation = w.Dropdown(
        options=['-','min', 'max', 'count', 'sum', 'mean'],
        description='Agg:',
        layout=w.Layout(width='max-content', max_width='120px', margin='1px 10px 2px 2px'),
        style={'description_width': 'initial'})
    checkbox_logx = w.Checkbox(
        value=False,
        disabled=False,
        indent=False,
        layout=w.Layout(width='max-content', margin='1px 2px 2px 2px', padding='2px 0 0 0'))
    checkbox_logy = w.Checkbox(
        value=False,
        disabled=False,
        indent=False,
        layout=w.Layout(width='max-content', margin='1px 2px 2px 2px', padding='2px 0 0 0'))

    
    # layout
    output_types = w.HBox(
        children=[
            btn_table, 
            btn_chart_line, 
            btn_chart_bar, 
            btn_chart_scatter
        ], 
        layout=w.Layout(align_items='center'))
    
    console_box = w.HBox([
        w.HBox([dropdown_x, dropdown_y, dropdown_aggregation]),
        w.HBox([w.Label('LogX:', layout=w.Layout(margin='0 5px 2px 2px')), checkbox_logx, w.Label('LogY:', layout=w.Layout(margin='0 5px 2px 2px')), checkbox_logy]),
    ])
    console = w.Output(
        layout=w.Layout(
            display='flex', 
            align_items='center', 
            margin='0px'))
    
    output = w.Output(style="width: 100%; padding: 0px")
    
    
    
    # event
    
    def on_btn_save_csv_clicked(b):
        b.icon = 'spinner spin'
        b.button_style = 'info'
        b.description = 'Saving'
        b.disabled = True
        
        output_dir = Path.cwd() / 'data-export'
        output_dir.mkdir(parents=True, exist_ok=True)
        output_filename = output_dir / f'{"" if not export_table_name else export_table_name + "_" }{datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}.csv'
        dataframe.to_csv(output_filename, index=False)
        
        b.icon = 'check'
        b.button_style = ''
        b.description = 'Saved'
        b.disabled = False
        b.tooltip = str(output_filename.resolve())
        
    btn_save_csv.on_click(on_btn_save_csv_clicked)
    
    def on_btn_render_clicked(b):
        for btn in [btn_table, btn_chart_line, btn_chart_bar, btn_chart_scatter]:
            btn.button_style = ''
        b.button_style = 'warning'
        ipd.display(ipd.Javascript("update_icon()"))
        
        if b.icon == 'table':
            with console:
                ipd.clear_output()
            with output:
                ipd.clear_output(wait=True)
                ipd.display(table_html) 
        else:
            state['current_render'] = b.icon.split('-')[0]
            with console:
                ipd.clear_output(wait=True)
                ipd.display(console_box)
            plot(
                output, 
                current_render=state['current_render'],
                template=state['template'],
                dataframe=dataframe,
                x=dropdown_x.value,
                y=dropdown_y.value,
                agg=dropdown_aggregation.value,
                logx=checkbox_logx.value,
                logy=checkbox_logy.value
            )
    btn_table.on_click(on_btn_render_clicked)
    btn_chart_line.on_click(on_btn_render_clicked)
    btn_chart_bar.on_click(on_btn_render_clicked)
    btn_chart_scatter.on_click(on_btn_render_clicked)
    
    def on_console_change(change):
        plot(
            output, 
            current_render=state['current_render'],
            template=state['template'],
            dataframe=dataframe,
            x=dropdown_x.value,
            y=dropdown_y.value,
            agg=dropdown_aggregation.value,
            logx=checkbox_logx.value,
            logy=checkbox_logy.value
        )
    dropdown_x.observe(on_console_change, 'value')
    dropdown_y.observe(on_console_change, 'value')
    dropdown_aggregation.observe(on_console_change, 'value')
    checkbox_logx.observe(on_console_change, 'value')
    checkbox_logy.observe(on_console_change, 'value')
    
        
    
    # render
    btn_table.click()

    return w.VBox([
        w.HBox(
            [output_types, w.HBox([console]), btn_save_csv], 
            layout=w.Layout(
                display='flex',
                justify_content='space-between',
                align_items='center',
            )
        ),
        output,
        w.HTML("""
        <script>
            function update_icon() {
                setTimeout(function () {
                    document.querySelectorAll(".fa-scatter-chart").forEach(element => {
                        let scatterIcon = `<span style="vertical-align:middle">
                                                <svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink" width="15px" height="11px" viewBox="0 0 15 11" version="1.1">
                                                    <g id="surface1">
                                                        <path fill="currentColor" d="M 0.0117188 0.0117188 L 0.976562 0.0117188 L 0.976562 10.960938 L 0.0117188 10.960938 Z M 0.0117188 0.0117188 "/>
                                                        <path fill="currentColor" d="M 0.0117188 9.96875 L 14.964844 9.96875 L 14.964844 10.960938 L 0.0117188 10.960938 Z M 0.0117188 9.96875 "/>
                                                        <path fill="currentColor" d="M 5.558594 7.476562 C 5.558594 6.652344 4.910156 5.984375 4.113281 5.984375 C 3.3125 5.984375 2.664062 6.652344 2.664062 7.476562 C 2.664062 8.304688 3.3125 8.972656 4.113281 8.972656 C 4.910156 8.972656 5.558594 8.304688 5.558594 7.476562 Z M 5.558594 7.476562 "/>
                                                        <path fill="currentColor" d="M 7.730469 3.246094 C 7.730469 2.421875 7.082031 1.753906 6.28125 1.753906 C 5.484375 1.753906 4.835938 2.421875 4.835938 3.246094 C 4.835938 4.074219 5.484375 4.742188 6.28125 4.742188 C 7.082031 4.742188 7.730469 4.074219 7.730469 3.246094 Z M 7.730469 3.246094 "/>
                                                        <path fill="currentColor" d="M 10.621094 6.484375 C 10.621094 5.660156 9.976562 4.988281 9.175781 4.988281 C 8.375 4.988281 7.730469 5.660156 7.730469 6.484375 C 7.730469 7.308594 8.375 7.976562 9.175781 7.976562 C 9.976562 7.976562 10.621094 7.308594 10.621094 6.484375 Z M 10.621094 6.484375 "/>
                                                        <path fill="currentColor" d="M 12.792969 2.253906 C 12.792969 1.425781 12.144531 0.757812 11.347656 0.757812 C 10.546875 0.757812 9.898438 1.425781 9.898438 2.253906 C 9.898438 3.078125 10.546875 3.746094 11.347656 3.746094 C 12.144531 3.746094 12.792969 3.078125 12.792969 2.253906 Z M 12.792969 2.253906 "/>
                                                    </g>
                                                </svg>
                                            </span>`
                        element.parentNode.innerHTML = scatterIcon;
                    });
                }, 0)
            }
            update_icon()
        </script>
        """)
    ])
