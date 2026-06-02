import axios, { AxiosError } from 'axios'

interface ApiErrorPayload {
  detail?: string
}

const isApiErrorPayload = (value: unknown): value is ApiErrorPayload => {
  if (typeof value !== 'object' || value === null) return false
  return 'detail' in value
}

const client = axios.create({
  baseURL: import.meta.env.VITE_API_URL ?? '/api',
  timeout: 15000,
})

client.interceptors.response.use(
  (response) => response,
  (error: AxiosError<unknown>) => {
    if (error.response?.status === 404) {
      return Promise.resolve({ data: null })
    }
    const payload = error.response?.data
    const message =
      isApiErrorPayload(payload) && typeof payload.detail === 'string'
        ? payload.detail
        : 'Lỗi kết nối máy chủ'
    return Promise.reject(new Error(message))
  },
)

export default client
