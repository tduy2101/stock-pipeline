import axios, { AxiosError } from 'axios'

interface ApiErrorPayload {
  detail?: string
}

const isApiErrorPayload = (value: unknown): value is ApiErrorPayload => {
  if (typeof value !== 'object' || value === null) return false
  return 'detail' in value
}

const resolveErrorMessage = (error: AxiosError<unknown>): string => {
  if (error.code === 'ECONNABORTED') {
    return 'Hết thời gian chờ phản hồi từ máy chủ. Vui lòng thử lại.'
  }
  if (!error.response) {
    return 'Không kết nối được backend. Kiểm tra FastAPI đang chạy tại port 8000.'
  }
  if (error.response.status === 404) {
    return 'Không tìm thấy dữ liệu cho yêu cầu này.'
  }
  if (error.response.status >= 500) {
    return 'Máy chủ đang gặp sự cố. Vui lòng thử lại sau.'
  }
  const payload = error.response.data
  if (isApiErrorPayload(payload) && typeof payload.detail === 'string') {
    return payload.detail
  }
  return 'Không thể tải dữ liệu. Vui lòng thử lại.'
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
    return Promise.reject(new Error(resolveErrorMessage(error)))
  },
)

export default client
