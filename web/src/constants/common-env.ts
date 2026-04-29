const webConfig = {
    // Priority: NEXT_PUBLIC_API_URL env var > dev default > empty (same-origin)
    // Set NEXT_PUBLIC_API_URL when the frontend and backend are deployed separately,
    // e.g. NEXT_PUBLIC_API_URL=https://your-backend.vercel.app
    apiUrl: process.env.NEXT_PUBLIC_API_URL || (process.env.NODE_ENV === 'development' ? 'http://127.0.0.1:8000' : ''),
    appVersion: process.env.NEXT_PUBLIC_APP_VERSION || '0.0.0',
}

export default webConfig
