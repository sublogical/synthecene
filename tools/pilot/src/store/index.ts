// Utilities
import { createPinia } from 'pinia'
import { reactive} from 'vue'

export default createPinia()

export const layout_state = reactive({
    drawer: false
  })
