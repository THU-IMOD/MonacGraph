<template>
  <div class="query-editor">
    <div class="editor-header">
      <h3>
        <el-icon><Edit /></el-icon>
        Query Editor
      </h3>
      <div class="editor-actions">
        <el-button 
          type="primary" 
          @click="executeQuery"
          :loading="store.isExecuting"
          :disabled="!store.isConnected || !localQuery.trim()"
        >
          <el-icon><CaretRight /></el-icon>
          Execute
        </el-button>
        <el-button 
          @click="clearQuery"
          :disabled="!localQuery"
        >
          <el-icon><Delete /></el-icon>
          Clear
        </el-button>
      </div>
    </div>

    <div class="editor-container">
      <!-- CodeMirror编辑器容器 -->
      <div ref="editorRef" class="code-editor"></div>
      <div class="editor-footer">
        <el-text size="small" type="info">
          Press Ctrl+Enter to execute
        </el-text>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, onUnmounted } from 'vue'
import { useGraphStore } from '@/store/graphStore'
import { Edit, CaretRight, Delete } from '@element-plus/icons-vue'

const store = useGraphStore()
const emit = defineEmits(['execute'])

// 编辑器引用
const editorRef = ref(null)
let editorView = null

// 本地查询文本
const localQuery = ref('')

// 初始化CodeMirror编辑器
const initEditor = async () => {
  if (!editorRef.value) return
  
  // 动态导入CodeMirror (使用CDN)
  if (!window.CodeMirror) {
    await loadCodeMirror()
  }
  
  // 创建编辑器实例
  const CodeMirror = window.CodeMirror
  editorView = CodeMirror(editorRef.value, {
    value: localQuery.value || '',
    mode: 'groovy',
    theme: 'default',
    lineNumbers: true,
    lineWrapping: true,
    indentUnit: 2,
    tabSize: 2,
    indentWithTabs: false,
    matchBrackets: true,
    autoCloseBrackets: true,
    styleActiveLine: true,
    extraKeys: {
      'Ctrl-Enter': () => executeQuery(),
      'Cmd-Enter': () => executeQuery()
    }
  })
  
  // 监听编辑器变化
  editorView.on('change', (cm) => {
    localQuery.value = cm.getValue()
  })
}

// 加载CodeMirror CDN
const loadCodeMirror = () => {
  return new Promise((resolve, reject) => {
    // 加载CSS
    const css = document.createElement('link')
    css.rel = 'stylesheet'
    css.href = 'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/codemirror.min.css'
    document.head.appendChild(css)
    
    // 加载JS
    const script = document.createElement('script')
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/codemirror.min.js'
    script.onload = () => {
      // 加载Groovy语法高亮（使用clike作为替代）
      const clikeScript = document.createElement('script')
      clikeScript.src = 'https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/mode/clike/clike.min.js'
      clikeScript.onload = () => {
        // 加载括号匹配和自动关闭
        Promise.all([
          loadScript('https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/addon/edit/matchbrackets.min.js'),
          loadScript('https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/addon/edit/closebrackets.min.js'),
          loadScript('https://cdnjs.cloudflare.com/ajax/libs/codemirror/5.65.2/addon/selection/active-line.min.js')
        ]).then(resolve)
      }
      clikeScript.onerror = reject
      document.head.appendChild(clikeScript)
    }
    script.onerror = reject
    document.head.appendChild(script)
  })
}

// 辅助函数：加载脚本
const loadScript = (src) => {
  return new Promise((resolve, reject) => {
    const script = document.createElement('script')
    script.src = src
    script.onload = resolve
    script.onerror = reject
    document.head.appendChild(script)
  })
}

// 监听 store 中的 currentQuery 变化
watch(() => store.currentQuery, (newQuery) => {
  if (newQuery) {
    localQuery.value = newQuery
    if (editorView) {
      editorView.setValue(newQuery)
    }
  }
}, { immediate: true })

// 执行查询
const executeQuery = () => {
  if (localQuery.value.trim()) {
    emit('execute', localQuery.value)
  }
}

// 清空查询
const clearQuery = () => {
  localQuery.value = ''
  if (editorView) {
    editorView.setValue('')
  }
  store.clearQueryResult?.()
}

// 组件挂载时初始化编辑器
onMounted(() => {
  initEditor()
})

// 组件卸载时清理
onUnmounted(() => {
  if (editorView) {
    editorView.toTextArea()
  }
})
</script>

<style scoped lang="scss">
.query-editor {
  .editor-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 10px;

    h3 {
      margin: 0;
      font-size: 18px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 10px;
      color: #2d3748;
      text-shadow: 0 1px 2px rgba(0, 0, 0, 0.05);
    }

    .editor-actions {
      display: flex;
      gap: 10px;
      
      .el-button {
        background: rgba(102, 126, 234, 0.1);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(102, 126, 234, 0.3);
        color: #667eea;
        font-weight: 600;
        border-radius: 10px;
        padding: 10px 20px;
        
        &:hover:not(:disabled) {
          background: rgba(102, 126, 234, 0.2);
          transform: translateY(-2px);
          box-shadow: 0 5px 15px rgba(102, 126, 234, 0.2);
        }
        
        &.el-button--primary {
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          border: none;
          box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
          color: white;
          
          &:hover {
            background: linear-gradient(135deg, #764ba2 0%, #f093fb 100%);
            box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6);
            transform: translateY(-3px) scale(1.02);
          }
          
          &:active {
            transform: translateY(-1px) scale(0.98);
          }
        }
        
        &:disabled {
          opacity: 0.5;
          cursor: not-allowed;
          transform: none !important;
        }
      }
    }
  }

  .editor-container {
    position: relative;
    
    .code-editor {
      border: 2px solid rgba(102, 126, 234, 0.2);
      border-radius: 12px;
      overflow: hidden;
      background: rgba(255, 255, 255, 0.9);
      backdrop-filter: blur(10px);
      box-shadow: inset 0 2px 4px rgba(0, 0, 0, 0.05);
      
      &:hover {
        border-color: rgba(102, 126, 234, 0.4);
      }
      
      :deep(.CodeMirror) {
        height: 120px;
        font-family: 'Fira Code', 'Courier New', Consolas, Monaco, monospace;
        font-size: 14px;
        line-height: 1.6;
        background: transparent;
        color: #2d3748;
        
        .CodeMirror-gutters {
          background: rgba(102, 126, 234, 0.05);
          border-right: 1px solid rgba(102, 126, 234, 0.2);
        }
        
        .CodeMirror-linenumber {
          color: #a0aec0;
          font-size: 12px;
        }
        
        .CodeMirror-cursor {
          border-left: 2px solid #667eea;
        }
        
        .CodeMirror-selected {
          background: rgba(102, 126, 234, 0.15);
        }
        
        .CodeMirror-activeline-background {
          background: rgba(102, 126, 234, 0.05);
        }
        
        .cm-keyword {
          color: #d73a49;
          font-weight: bold;
        }
        
        .cm-string {
          color: #22863a;
        }
        
        .cm-comment {
          color: #6a737d;
          font-style: italic;
        }
        
        .cm-number {
          color: #005cc5;
        }
        
        .cm-def {
          color: #6f42c1;
        }
        
        .cm-operator {
          color: #d73a49;
        }
        
        .cm-variable {
          color: #24292e;
        }
        
        .cm-property {
          color: #005cc5;
        }
      }
    }

    .editor-footer {
      margin-top: 6px;
      text-align: right;
      
      .el-text {
        color: #718096;
        font-size: 12px;
        font-weight: 500;
        background: rgba(102, 126, 234, 0.08);
        padding: 4px 10px;
        border-radius: 20px;
        backdrop-filter: blur(5px);
      }
    }
  }
}
</style>