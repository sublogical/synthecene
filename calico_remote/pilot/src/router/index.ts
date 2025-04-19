// Composables
import { createRouter, createWebHistory } from 'vue-router'

const routes = [
  {
    path: '/',
    component: () => import('@/layouts/default/Default.vue'),
    children: [
      {
        path: '',
        name: 'Home',
        // route level code-splitting
        // this generates a separate chunk (about.[hash].js) for this route
        // which is lazy-loaded when the route is visited.
        component: () => import(/* webpackChunkName: "home" */ '@/views/Home.vue'),
      },
      {
        path: 'annotation',
        name: 'Annotation',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Annotation.vue'),
      },
      {
        path: 'catalog',
        name: 'Catalog',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Catalog.vue'),
      },
      {
        path: 'chat',
        name: 'Chat',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Chat.vue'),
      },
      {
        path: 'crawler',
        name: 'Crawler',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Crawler.vue'),
      },
      {
        path: 'models',
        name: 'Models',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Models.vue'),
      },
      {
        path: 'notifications',
        name: 'Notifications',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Notifications.vue'),
      },
      {
        path: 'people',
        name: 'People',
        component: () => import(/* webpackChunkName: "people" */ '@/views/People.vue'),
      },
      {
        path: 'settings',
        name: 'Settings',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Settings.vue'),
      },
      {
        path: 'tasks',
        name: 'Tasks',
        component: () => import(/* webpackChunkName: "people" */ '@/views/Tasks.vue'),
      },
    ],
  },
]

const router = createRouter({
  history: createWebHistory(process.env.BASE_URL),
  routes,
})

export default router
