<template>
  <div class="graph-uploader">
    <el-dialog
      v-model="dialogVisible"
      title="Create New Graph"
      width="550px"
      :close-on-click-modal="false"
    >
      <el-form
        ref="formRef"
        :model="formData"
        :rules="formRules"
        label-width="140px"
        label-position="left"
      >
        <!-- Info Alert -->
        <el-alert
          type="info"
          :closable="false"
          show-icon
          style="margin-bottom: 20px;"
        >
          <template #title>
            <span style="font-size: 13px;">Server will automatically initialize the graph after upload</span>
          </template>
        </el-alert>

        <!-- Graph Name -->
        <el-form-item label="Graph Name" prop="graphName">
          <el-input
            v-model="formData.graphName"
            placeholder="e.g., example"
            clearable
          >
            <template #prepend>graph =</template>
          </el-input>
          <div class="help-text">
            Name for CommunityGraph.open('{{ formData.graphName || "name" }}')
          </div>
        </el-form-item>

        <!-- Graph Topology File (.graph) -->
        <el-form-item label="Graph Topology">
          <el-upload
            ref="graphUploadRef"
            :auto-upload="false"
            :limit="1"
            accept=".graph"
            :on-change="handleGraphFileChange"
            :on-remove="handleGraphFileRemove"
            :on-exceed="handleGraphFileExceed"
            :file-list="graphFileList"
          >
            <el-button type="primary" :icon="Upload">
              Select .graph File
            </el-button>
            <template #tip>
              <div class="upload-tip">
                Graph topology file (optional, .graph)
              </div>
            </template>
          </el-upload>
        </el-form-item>

        <!-- Vertex Properties File -->
        <el-form-item label="Vertex Properties" prop="vertexFile">
          <el-upload
            ref="vertexUploadRef"
            :auto-upload="false"
            :limit="1"
            accept=".json,.csv"
            :on-change="handleVertexFileChange"
            :on-remove="handleVertexFileRemove"
            :on-exceed="handleVertexFileExceed"
            :file-list="vertexFileList"
          >
            <el-button type="primary" :icon="Upload">
              Select .json/.csv File
            </el-button>
            <template #tip>
              <div class="upload-tip">
                Vertex property file (optional, .json or .csv)
              </div>
            </template>
          </el-upload>
        </el-form-item>

        <!-- Edge Properties File -->
        <el-form-item label="Edge Properties" prop="edgeFile">
          <el-upload
            ref="edgeUploadRef"
            :auto-upload="false"
            :limit="1"
            accept=".json,.csv"
            :on-change="handleEdgeFileChange"
            :on-remove="handleEdgeFileRemove"
            :on-exceed="handleEdgeFileExceed"
            :file-list="edgeFileList"
          >
            <el-button type="primary" :icon="Upload">
              Select .json/.csv File
            </el-button>
            <template #tip>
              <div class="upload-tip">
                Edge property file (optional, .json or .csv)
              </div>
            </template>
          </el-upload>
        </el-form-item>
      </el-form>

      <template #footer>
        <div class="dialog-footer">
          <el-button @click="handleCancel" size="large">Cancel</el-button>
          <el-button
            type="primary"
            @click="handleSubmit"
            :loading="uploading"
            :icon="Check"
            size="large"
          >
            Upload
          </el-button>
        </div>
      </template>
    </el-dialog>
  </div>
</template>

<script setup>
import { ref, computed, watch } from 'vue'
import { ElMessage, ElMessageBox } from 'element-plus'
import { Upload, Check } from '@element-plus/icons-vue'
import { useGraphStore } from '../store/graphStore'

const props = defineProps({
  visible: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['update:visible', 'success'])

const store = useGraphStore()
const formRef = ref(null)
const graphUploadRef = ref(null)
const vertexUploadRef = ref(null)
const edgeUploadRef = ref(null)

// Form data
const formData = ref({
  graphName: '',
  graphFile: null,
  vertexFile: null,
  edgeFile: null
})

// File lists for upload components
const graphFileList = ref([])
const vertexFileList = ref([])
const edgeFileList = ref([])

// Upload state
const uploading = ref(false)

// Dialog visibility
const dialogVisible = computed({
  get: () => props.visible,
  set: (val) => emit('update:visible', val)
})

// Form validation rules
const formRules = {
  graphName: [
    { required: true, message: 'Please enter graph name', trigger: 'blur' },
    { 
      pattern: /^[a-zA-Z][a-zA-Z0-9_-]*$/,
      message: 'Name must start with letter and contain only letters, numbers, underscore, hyphen',
      trigger: 'blur'
    }
  ]
  // graphFile is now optional - you can reload existing graphs
}

// File change handlers
const handleGraphFileChange = (file) => {
  formData.value.graphFile = file.raw
  graphFileList.value = [file]
}

const handleGraphFileRemove = () => {
  formData.value.graphFile = null
  graphFileList.value = []
}

const handleVertexFileChange = (file) => {
  formData.value.vertexFile = file.raw
  vertexFileList.value = [file]
}

const handleVertexFileRemove = () => {
  formData.value.vertexFile = null
  vertexFileList.value = []
}

const handleEdgeFileChange = (file) => {
  formData.value.edgeFile = file.raw
  edgeFileList.value = [file]
}

const handleEdgeFileRemove = () => {
  formData.value.edgeFile = null
  edgeFileList.value = []
}

// Handle file exceed (replace instead of add)
const handleGraphFileExceed = (files) => {
  // Clear existing file
  graphUploadRef.value.clearFiles()
  // Add new file
  const file = files[0]
  formData.value.graphFile = file
  graphFileList.value = [{ name: file.name, raw: file }]
}

const handleVertexFileExceed = (files) => {
  // Clear existing file
  vertexUploadRef.value.clearFiles()
  // Add new file
  const file = files[0]
  formData.value.vertexFile = file
  vertexFileList.value = [{ name: file.name, raw: file }]
}

const handleEdgeFileExceed = (files) => {
  // Clear existing file
  edgeUploadRef.value.clearFiles()
  // Add new file
  const file = files[0]
  formData.value.edgeFile = file
  edgeFileList.value = [{ name: file.name, raw: file }]
}

// Handle cancel
const handleCancel = () => {
  dialogVisible.value = false
  resetForm()
}

// Reset form
const resetForm = () => {
  formRef.value?.resetFields()
  formData.value = {
    graphName: '',
    graphFile: null,
    vertexFile: null,
    edgeFile: null
  }
  graphFileList.value = []
  vertexFileList.value = []
  edgeFileList.value = []
}

// Handle submit
const handleSubmit = async () => {
  try {
    // Validate form
    await formRef.value.validate()
    
    // Confirm upload
    await ElMessageBox.confirm(
      `This will upload files and create graph '${formData.value.graphName}'. Continue?`,
      'Confirm Upload',
      {
        type: 'warning',
        confirmButtonText: 'Upload',
        cancelButtonText: 'Cancel'
      }
    )
    
    uploading.value = true
    
    try {
      // Upload files
      await uploadFiles()
      
      // Server automatically executes initialization
      // No need to execute it again from frontend
      
      ElMessage.success({
        message: 'Graph uploaded successfully! Server has automatically initialized the graph. You can query directly.',
        duration: 5000
      })
      
      emit('success', {
        graphName: formData.value.graphName
      })
      
      dialogVisible.value = false
      resetForm()
    } catch (error) {
      console.error('Upload failed:', error)
      ElMessage.error(error.message || 'Failed to upload graph')
    } finally {
      uploading.value = false
    }
  } catch (error) {
    // Handle validation or confirmation cancel
    if (error !== 'cancel') {
      console.error('Error:', error)
    }
  }
}

// Upload files to server
const uploadFiles = async () => {
  const formDataToSend = new FormData()
  
  // Add graph name
  formDataToSend.append('graphName', formData.value.graphName)
  
  // Add files (all optional now)
  if (formData.value.graphFile) {
    formDataToSend.append('graphFile', formData.value.graphFile)
  }
  
  if (formData.value.vertexFile) {
    formDataToSend.append('vertexFile', formData.value.vertexFile)
  }
  
  if (formData.value.edgeFile) {
    formDataToSend.append('edgeFile', formData.value.edgeFile)
  }
  
  // Send to server
  const response = await fetch('http://localhost:8284/api/graph/upload', {
    method: 'POST',
    body: formDataToSend
  })
  
  if (!response.ok) {
    const error = await response.json()
    throw new Error(error.message || 'Upload failed')
  }
  
  return await response.json()
}

// Watch dialog visibility to reset form when closed
watch(dialogVisible, (visible) => {
  if (!visible) {
    resetForm()
  }
})
</script>

<style scoped lang="scss">
.graph-uploader {
  :deep(.el-dialog) {
    border-radius: 20px;
    overflow: hidden;
    
    .el-dialog__header {
      background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
      padding: 20px 24px;
      border-bottom: 1px solid rgba(0, 0, 0, 0.08);
      
      .el-dialog__title {
        font-size: 20px;
        font-weight: 700;
        color: #2d3748;
      }
    }
    
    .el-dialog__body {
      padding: 24px;
    }
    
    .el-dialog__footer {
      padding: 16px 24px;
      border-top: 1px solid rgba(0, 0, 0, 0.08);
      background: rgba(0, 0, 0, 0.02);
    }
  }
  
  :deep(.el-form) {
    .el-form-item {
      margin-bottom: 20px;
      
      .el-form-item__label {
        font-weight: 600;
        color: #2d3748;
        font-size: 14px;
      }
      
      .el-form-item__content {
        .el-input__wrapper {
          border-radius: 8px;
          box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
          transition: all 0.3s;
          
          &:hover {
            box-shadow: 0 2px 6px rgba(102, 126, 234, 0.2);
          }
          
          &.is-focus {
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.15);
          }
        }
        
        .el-input-group__prepend {
          background: rgba(102, 126, 234, 0.08);
          border-right: 1px solid rgba(102, 126, 234, 0.2);
          font-weight: 600;
          color: #667eea;
        }
      }
    }
  }
  
  .help-text {
    font-size: 12px;
    color: #909399;
    margin-top: 6px;
    line-height: 1.5;
  }
  
  .upload-tip {
    font-size: 12px;
    color: #909399;
    margin-top: 6px;
    line-height: 1.5;
  }
  
  // 重点修复：确保上传组件容器满宽，按钮左对齐
  :deep(.el-upload) {
    width: 100% !important; // 强制容器满宽，避免宽度不足导致对齐偏差
    text-align: left !important; // 强制容器内内容左对齐
    display: block !important; // 改为块级元素，确保宽度生效
    padding: 0 !important; // 清除组件默认内边距
    margin: 0 !important; // 清除组件默认外边距
    
    // 确保按钮左对齐，无额外间距
    .el-button {
      margin: 0 !important; // 清除按钮默认外边距
      padding: 10px 20px !important; // 保持原有按钮内边距，确保样式统一
      text-align: left !important; // 按钮文字左对齐（可选，根据需求调整）
    }
    
    // 确保文件列表也左对齐，与按钮保持一致
    .el-upload-list {
      margin-top: 10px !important;
      padding: 0 !important;
      text-align: left !important;
      
      .el-upload-list__item {
        border-radius: 8px;
        border: 1px solid rgba(102, 126, 234, 0.2);
        background: rgba(102, 126, 234, 0.05);
        
        &:hover {
          background: rgba(102, 126, 234, 0.1);
        }
      }
    }
    
    // 清除上传组件的默认文本对齐偏差
    .el-upload__text,
    .el-upload__tip {
      text-align: left !important;
      margin: 0 !important;
    }
  }
  
  // 补充：确保表单内容区域无默认内边距，与 label 对齐
  :deep(.el-form-item__content) {
    padding-left: 0 !important; // 清除内容区域默认左内边距，与 label 右侧对齐
  }
  
  :deep(.el-button) {
    border-radius: 8px;
    font-weight: 600;
    padding: 10px 20px;
    transition: all 0.3s;
    
    &.el-button--primary {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      border: none;
      
      &:hover:not(:disabled) {
        background: linear-gradient(135deg, #764ba2 0%, #f093fb 100%);
        transform: translateY(-2px);
        box-shadow: 0 5px 15px rgba(102, 126, 234, 0.4);
      }
      
      &:active:not(:disabled) {
        transform: translateY(-1px);
      }
    }
    
    &:not(.el-button--primary) {
      background: rgba(0, 0, 0, 0.05);
      border: 1px solid rgba(0, 0, 0, 0.1);
      
      &:hover:not(:disabled) {
        background: rgba(0, 0, 0, 0.08);
        border-color: rgba(0, 0, 0, 0.15);
      }
    }
  }
  
  .dialog-footer {
    display: flex;
    justify-content: flex-end;
    gap: 12px;
  }
}
</style>
