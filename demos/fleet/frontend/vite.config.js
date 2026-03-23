import { defineConfig } from 'vite';

export default defineConfig({
    server: {
        host: '0.0.0.0',
        port: 5173,
        proxy: {
            '/api': {
                target: 'http://postgrest:3000',
                rewrite: (path) => path.replace(/^\/api/, ''),
                changeOrigin: true,
            },
        },
    },
});
