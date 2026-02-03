<template>
  <transition name="dialog-fade">
    <div v-if="modelValue" class="connection-overlay" @click.self="handleClose">
      <div class="connection-dialog">
        <!-- Header -->
        <div class="dialog-header">
          <h3 class="dialog-title">Connect to MonacGraph Server</h3>
          <button class="close-btn" @click="handleClose">
            <el-icon><Close /></el-icon>
          </button>
        </div>

        <!-- Body -->
        <div class="dialog-body">
          <!-- Info Alert -->
          <div class="info-alert">
            <el-icon class="info-icon"><InfoFilled /></el-icon>
            <span>Enter server connection details</span>
          </div>

          <!-- Form -->
          <div class="form-group">
            <label class="form-label">Host</label>
            <div class="input-wrapper">
              <el-icon class="input-icon"><Connection /></el-icon>
              <input
                v-model="formData.host"
                type="text"
                class="form-input with-icon"
                placeholder="localhost"
              />
            </div>
          </div>

          <div class="form-group">
            <label class="form-label">Port</label>
            <div class="input-wrapper">
              <input
                v-model.number="formData.port"
                type="number"
                class="form-input"
                placeholder="8182"
                min="1"
                max="65535"
              />
            </div>
          </div>
        </div>

        <!-- Footer -->
        <div class="dialog-footer">
          <button class="btn btn-cancel" @click="handleClose">
            Cancel
          </button>
          <button 
            class="btn btn-primary" 
            @click="handleConnect"
            :disabled="isConnecting"
          >
            <el-icon v-if="!isConnecting"><Link /></el-icon>
            <span>{{ isConnecting ? 'Connecting...' : 'Connect' }}</span>
          </button>
        </div>
      </div>
    </div>
  </transition>
</template>

<script setup>
import { ref, watch } from 'vue'
import { Close, InfoFilled, Connection, Link } from '@element-plus/icons-vue'

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false
  },
  initialHost: {
    type: String,
    default: 'localhost'
  },
  initialPort: {
    type: Number,
    default: 8182
  },
  isConnecting: {
    type: Boolean,
    default: false
  }
})

const emit = defineEmits(['update:modelValue', 'connect'])

const formData = ref({
  host: props.initialHost,
  port: props.initialPort
})

// Watch for prop changes
watch(() => props.initialHost, (val) => {
  formData.value.host = val
})

watch(() => props.initialPort, (val) => {
  formData.value.port = val
})

const handleClose = () => {
  emit('update:modelValue', false)
}

const handleConnect = () => {
  emit('connect', {
    host: formData.value.host,
    port: formData.value.port
  })
}
</script>

<style scoped lang="scss">
.connection-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(4px);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 2000;
}

.connection-dialog {
  width: 480px;
  background: rgba(255, 255, 255, 0.98);
  backdrop-filter: blur(30px);
  border-radius: 20px;
  box-shadow: 0 25px 50px rgba(0, 0, 0, 0.3);
  overflow: hidden;
  animation: dialog-in 0.3s ease-out;
}

@keyframes dialog-in {
  from {
    opacity: 0;
    transform: scale(0.9) translateY(-20px);
  }
  to {
    opacity: 1;
    transform: scale(1) translateY(0);
  }
}

.dialog-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 20px 28px;
  background: linear-gradient(135deg, rgba(102, 126, 234, 0.1) 0%, rgba(118, 75, 162, 0.1) 100%);
  border-bottom: 1px solid rgba(0, 0, 0, 0.08);

  .dialog-title {
    margin: 0;
    font-size: 20px;
    font-weight: 700;
    color: #2d3748;
  }

  .close-btn {
    width: 32px;
    height: 32px;
    border: none;
    background: rgba(0, 0, 0, 0.05);
    border-radius: 8px;
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #909399;
    transition: all 0.2s;

    &:hover {
      background: rgba(0, 0, 0, 0.1);
      color: #2d3748;
    }

    .el-icon {
      font-size: 18px;
    }
  }
}

.dialog-body {
  padding: 28px;
}

.info-alert {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 12px 16px;
  background: rgba(102, 126, 234, 0.08);
  border: 1px solid rgba(102, 126, 234, 0.2);
  border-radius: 10px;
  margin-bottom: 24px;
  color: #667eea;
  font-size: 13px;

  .info-icon {
    font-size: 16px;
    flex-shrink: 0;
  }
}

.form-group {
  margin-bottom: 20px;

  &:last-child {
    margin-bottom: 0;
  }
}

.form-label {
  display: block;
  margin-bottom: 8px;
  font-size: 14px;
  font-weight: 600;
  color: #2d3748;
}

.input-wrapper {
  position: relative;
  display: flex;
  align-items: center;

  .input-icon {
    position: absolute;
    left: 12px;
    font-size: 18px;
    color: #667eea;
    pointer-events: none;
    z-index: 1;
  }
}

.form-input {
  width: 100%;
  height: 44px;
  padding: 0 16px;
  font-size: 14px;
  color: #2d3748;
  background: rgba(255, 255, 255, 0.5);
  border: 1px solid rgba(102, 126, 234, 0.3);
  border-radius: 10px;
  outline: none;
  transition: all 0.3s;
  box-shadow: 0 2px 6px rgba(0, 0, 0, 0.08);

  &:hover {
    background: rgba(255, 255, 255, 0.7);
    border-color: rgba(102, 126, 234, 0.5);
    box-shadow: 0 3px 10px rgba(102, 126, 234, 0.2);
  }

  &:focus {
    background: rgba(255, 255, 255, 0.9);
    border-color: #667eea;
    box-shadow: 0 0 0 4px rgba(102, 126, 234, 0.15);
  }

  &::placeholder {
    color: #a0aec0;
  }

  // For Host input with icon
  &.with-icon {
    padding-left: 44px;
  }

  // Remove number input arrows
  &[type="number"] {
    -moz-appearance: textfield;
    
    &::-webkit-outer-spin-button,
    &::-webkit-inner-spin-button {
      -webkit-appearance: none;
      margin: 0;
    }
  }
}

.dialog-footer {
  display: flex;
  justify-content: flex-end;
  gap: 12px;
  padding: 20px 28px;
  border-top: 1px solid rgba(0, 0, 0, 0.08);
  background: rgba(0, 0, 0, 0.02);
}

.btn {
  height: 44px;
  padding: 0 24px;
  font-size: 14px;
  font-weight: 600;
  border: none;
  border-radius: 10px;
  cursor: pointer;
  transition: all 0.3s;
  display: flex;
  align-items: center;
  gap: 8px;

  &:disabled {
    opacity: 0.6;
    cursor: not-allowed;
  }

  &.btn-cancel {
    background: rgba(0, 0, 0, 0.05);
    color: #2d3748;
    border: 1px solid rgba(0, 0, 0, 0.1);

    &:hover:not(:disabled) {
      background: rgba(0, 0, 0, 0.08);
      border-color: rgba(0, 0, 0, 0.15);
      transform: translateY(-1px);
    }
  }

  &.btn-primary {
    background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
    color: white;

    &:hover:not(:disabled) {
      background: linear-gradient(135deg, #764ba2 0%, #f093fb 100%);
      transform: translateY(-2px);
      box-shadow: 0 6px 20px rgba(102, 126, 234, 0.4);
    }

    &:active:not(:disabled) {
      transform: translateY(-1px);
    }

    .el-icon {
      font-size: 16px;
    }
  }
}

// Transition
.dialog-fade-enter-active,
.dialog-fade-leave-active {
  transition: opacity 0.3s ease;

  .connection-dialog {
    transition: all 0.3s ease;
  }
}

.dialog-fade-enter-from,
.dialog-fade-leave-to {
  opacity: 0;

  .connection-dialog {
    transform: scale(0.9) translateY(-20px);
  }
}
</style>
