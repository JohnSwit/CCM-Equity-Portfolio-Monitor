/** @type {import('next').NextConfig} */
const nextConfig = {
  reactStrictMode: true,
  swcMinify: true,
  compress: true,
  env: {
    NEXT_PUBLIC_API_URL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000',
  },
  experimental: {
    optimizePackageImports: ['recharts', 'date-fns', 'react-select'],
  },
}

module.exports = nextConfig
