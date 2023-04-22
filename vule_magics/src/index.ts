import {
  JupyterFrontEnd,
  JupyterFrontEndPlugin
} from '@jupyterlab/application';
import { INotebookTracker } from '@jupyterlab/notebook';
import { CodeMirrorEditor } from '@jupyterlab/codemirror';
import { Cell, isCodeCellModel } from '@jupyterlab/cells';
import CodeMirror from 'codemirror';

interface IMagicSyntax {
  [key: string]: string;
}

const CellMagicSyntaxMap: IMagicSyntax = {
  default: 'text/x-ipython',
  '%%sql': 'text/x-sparksql',
  '%%sh': 'text/x-sh',
  '%%bash': 'text/x-sh'
};

class SyntaxHighlighter {
  constructor(
    protected app: JupyterFrontEnd,
    protected tracker: INotebookTracker
  ) {
    this.tracker.currentChanged.connect(() => {
      // console.log('changed!');
      if (!this.tracker.currentWidget) {
        return;
      }
      const notebook = this.tracker.currentWidget;
      notebook.content.modelContentChanged.connect(() => {
        if (notebook.content.widgets.length > 0) {
          // console.log('tracker.widgets:', notebook.content.widgets.length);
          notebook.content.widgets.forEach((cell: Cell) => {
            this.configCellEditor(cell);
          });
        }
      });
    });
  }

  private configCellEditor(cell: Cell): void {
    if (cell !== null && isCodeCellModel(cell.model)) {
      const editor = this.getCellEditor(cell);
      this.cellMagicSyntaxMap(editor);
    }
  }

  private getCellEditor(cell: Cell): CodeMirror.Editor {
    return (cell.editor as CodeMirrorEditor).editor;
  }

  private cellMagicSyntaxMap(cellEditor: CodeMirror.Editor): void {
    const magic = cellEditor.getDoc().getLine(0).split(' ')[0];
    if (magic.startsWith('%%') && magic in CellMagicSyntaxMap) {
      this.highlight(cellEditor, true, CellMagicSyntaxMap[magic]);
      return;
    }
    this.highlight(cellEditor, true, CellMagicSyntaxMap.default);
  }

  private highlight(
    cellEditor: CodeMirror.Editor,
    retry = true,
    mode: string
  ): void {
    const current_mode = cellEditor.getOption('mode') as string;
    // console.log('current_mode:', current_mode);

    if (current_mode === 'null') {
      if (retry) {
        // putting at the end of execution queue to allow the CodeMirror mode to be updated
        setTimeout(() => this.highlight(cellEditor, false, mode), 0);
      }
      return;
    }
    // console.log('current_mode:', current_mode);
    cellEditor.setOption('mode', mode);
  }
}

/**
 * Activate extension
 */
function activate(app: JupyterFrontEnd, tracker: INotebookTracker): void {
  console.log('JupyterLab extension vule-magics is activated!');
  const sh = new SyntaxHighlighter(app, tracker);
  console.log('SyntaxHighlighter Loaded ', sh);
}

/**
 * Initialization data for the jupyterlab_spellchecker extension.
 */
const plugin: JupyterFrontEndPlugin<void> = {
  id: 'vule-magics:plugin',
  autoStart: true,
  requires: [INotebookTracker],
  activate: activate
};

export default plugin;
