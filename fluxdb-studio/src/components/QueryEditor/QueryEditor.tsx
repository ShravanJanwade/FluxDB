// QueryEditor Component - Monaco editor for SQL queries
import { useCallback, useRef } from 'react';
import Editor, { OnMount, Monaco } from '@monaco-editor/react';
import { useQueryStore } from '../../stores/queryStore';
import { QueryToolbar } from './QueryToolbar';

export function QueryEditor() {
  const { currentQuery, setCurrentQuery, executeQuery, isExecuting } = useQueryStore();
  const editorRef = useRef<any>(null);
  const monacoRef = useRef<Monaco | null>(null);

  const handleEditorMount: OnMount = (editor, monaco) => {
    editorRef.current = editor;
    monacoRef.current = monaco;

    // Configure SQL language features
    monaco.languages.registerCompletionItemProvider('sql', {
      provideCompletionItems: (model: any, position: any) => {
        const suggestions = [
          // Keywords
          ...['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT', 'AND', 'OR', 'AS', 'DESC', 'ASC'].map(
            (keyword) => ({
              label: keyword,
              kind: monaco.languages.CompletionItemKind.Keyword,
              insertText: keyword,
              range: {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: position.column,
                endColumn: position.column,
              },
            })
          ),
          // Aggregate functions
          ...['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'MEAN', 'FIRST', 'LAST'].map(
            (func) => ({
              label: func,
              kind: monaco.languages.CompletionItemKind.Function,
              insertText: `${func}($0)`,
              insertTextRules: monaco.languages.CompletionItemInsertTextRule.InsertAsSnippet,
              range: {
                startLineNumber: position.lineNumber,
                endLineNumber: position.lineNumber,
                startColumn: position.column,
                endColumn: position.column,
              },
            })
          ),
          // Time functions
          ...['now()', 'time(1h)', 'time(1d)', 'time(1m)'].map((func) => ({
            label: func,
            kind: monaco.languages.CompletionItemKind.Function,
            insertText: func,
            range: {
              startLineNumber: position.lineNumber,
              endLineNumber: position.lineNumber,
              startColumn: position.column,
              endColumn: position.column,
            },
          })),
        ];

        return { suggestions };
      },
    });

    // Add keyboard shortcuts
    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyCode.Enter, () => {
      executeQuery();
    });

    editor.addCommand(monaco.KeyMod.CtrlCmd | monaco.KeyMod.Shift | monaco.KeyCode.KeyF, () => {
      editor.getAction('editor.action.formatDocument')?.run();
    });

    // Focus editor
    editor.focus();
  };

  const handleChange = useCallback((value: string | undefined) => {
    setCurrentQuery(value || '');
  }, [setCurrentQuery]);

  return (
    <div className="query-editor">
      <QueryToolbar />
      
      <div className="monaco-container">
        <Editor
          height="100%"
          defaultLanguage="sql"
          value={currentQuery}
          onChange={handleChange}
          onMount={handleEditorMount}
          theme="fluxdb-dark"
          beforeMount={(monaco) => {
            // Define custom theme
            monaco.editor.defineTheme('fluxdb-dark', {
              base: 'vs-dark',
              inherit: true,
              rules: [
                { token: 'keyword', foreground: '8b5cf6', fontStyle: 'bold' },
                { token: 'string', foreground: '10b981' },
                { token: 'number', foreground: 'f59e0b' },
                { token: 'comment', foreground: '64748b', fontStyle: 'italic' },
                { token: 'operator', foreground: '3b82f6' },
              ],
              colors: {
                'editor.background': '#0f172a',
                'editor.foreground': '#f1f5f9',
                'editor.lineHighlightBackground': '#1e293b',
                'editor.selectionBackground': '#334155',
                'editorCursor.foreground': '#3b82f6',
                'editorLineNumber.foreground': '#64748b',
                'editorLineNumber.activeForeground': '#f1f5f9',
                'editor.inactiveSelectionBackground': '#1e293b',
              },
            });
          }}
          options={{
            minimap: { enabled: false },
            fontSize: 14,
            fontFamily: "'JetBrains Mono', 'Fira Code', 'Consolas', monospace",
            lineNumbers: 'on',
            roundedSelection: true,
            scrollBeyondLastLine: false,
            wordWrap: 'on',
            automaticLayout: true,
            tabSize: 2,
            padding: { top: 16, bottom: 16 },
            suggestOnTriggerCharacters: true,
            quickSuggestions: true,
            folding: true,
            renderLineHighlight: 'all',
            cursorBlinking: 'smooth',
            cursorSmoothCaretAnimation: 'on',
            smoothScrolling: true,
            contextmenu: true,
          }}
          loading={
            <div className="editor-loading">
              <div className="loading-spinner" />
              <span>Loading editor...</span>
            </div>
          }
        />
      </div>

      {isExecuting && (
        <div className="query-executing-overlay">
          <div className="loading-spinner" />
          <span>Executing query...</span>
        </div>
      )}
    </div>
  );
}
