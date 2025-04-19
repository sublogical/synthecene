// Utilities
import { defineStore } from 'pinia'

export const useAppStore = defineStore('app', {
  state: () => ({
    notification_count: 0,
    chat_count: 0,
    task_count: 0,
  }),
})
